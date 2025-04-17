# SPDX-License-Identifier: MIT
# Copyright (C) 2019 Thomas Rausch and faas-sim authors
# Copyright (C) 2025 Natalie Balashov

import time
import csv
import sys
import math
import logging
from typing import List, Dict
from collections import defaultdict
from enum import Enum
import pandas as pd

import simpy
from start_delayed import start_delayed

from ether.core import Node, Capacity

from ether.topology import Topology
from simlogging import RuntimeLogger, NullLogger

import params
from example_topology import ExampleScenario


logger = logging.getLogger(__name__)


def main(workload_filename):
    logging.basicConfig(filename='out.log', level=logging.DEBUG)

    topology = create_topology()
    benchmark = Benchmark(workload_filename)
    sim = Simulation(topology, benchmark)
    sim.run()

    df = sim.env.metrics.extract_dataframe('enqueue_timestamp')
    df.to_csv(f'df_enqueue.csv', sep=',')

    df = sim.env.metrics.extract_dataframe('dequeue_timestamp')
    df.to_csv(f'df_dequeue.csv', sep=',')

    df = sim.env.metrics.extract_dataframe('exec_start_time')
    df.to_csv(f'df_cpu_use.csv', sep=',')

    df = sim.env.metrics.extract_dataframe('queuelength')
    df.to_csv(f'df_queuelength.csv', sep=',')

    df = sim.env.metrics.extract_dataframe('number_requests_in_flight')
    df.to_csv(f'df_number_requests_in_flight.csv', sep=',')

    df = sim.env.metrics.extract_dataframe('delta_exec')
    df.to_csv(f'df_delta_exec.csv', sep=',')


def create_topology() -> Topology:
    t = Topology()
    ExampleScenario().materialize(t)
    return t


def counter(start: int = 1):
    n = start
    while True:
        yield n
        n += 1


class Environment(simpy.Environment):
    def __init__(self, initial_time=0):
        super().__init__(initial_time)
        self.topology = None
        self.benchmark = None
        self.metrics = None
        self.node_states = list()


def ilogger(env: Environment, app_name: str, msg: str) -> None:
    logger.info(f'[simtime={env.now} ms]:<{app_name}>:{msg}')

def dlogger(env: Environment, app_name: str, msg: str) -> None:
    logger.debug(f'[simtime={env.now} ms]:<{app_name}>:{msg}')

def wlogger(env: Environment, app_name: str, msg: str) -> None:
    logger.warning(f'[simtime={env.now} ms]:<{app_name}>:{msg}')

class FRState(Enum):
    ARRIVED = 1
    SCHEDULED = 2
    SETUP = 3
    RUNNING = 4
    COMPLETE = 5

class Request:
    request_id: int
    name: str
    exec_time: int
    start_ts: int
    end_ts: int
    delta_exec_time: int
    mem_use: int
    core_util: int
    id_generator = counter()
    fr_state: int

    def __init__(self, name, exec_time, mem_use, core_use) -> None:
        super().__init__()
        self.request_id = next(self.id_generator)
        self.name = name
        self.exec_time = exec_time
        self.start_ts = -1
        self.end_ts = -1
        self.delta_exec_time = 0
        self.mem_use = mem_use
        self.core_util = core_use
        self.fr_state = FRState.ARRIVED.value

    def __str__(self) -> str:
        return 'Request(%d, %s)' % (self.request_id, self.name)

    def __repr__(self):
        return self.__str__()


class Instance:
    is_idle: bool
    request: Request
    previous_requests: List[Request]

    def __init__(self, request: Request):
        self.is_idle = False
        self.request = request
        self.previous_requests = list()

    def accept_new_request(self, new_request: Request):
        assert new_request.name == self.request.name # caller should do this check, but add safeguard here just in case
        self.previous_requests.append(self.request)
        self.request = new_request
        self.is_idle = False

    def idle(self, env: Environment):
        self.is_idle = True
        ilogger(env, self.request.name, f'Instance for request={self.request.request_id} will idle for up to {params.INSTANCE_IDLE_TIME} ms')
        yield env.timeout(params.INSTANCE_IDLE_TIME)
        self.is_idle = False


class NodeState:
    env: Environment
    ether_node: Node
    instances: Dict[Request, Instance]
    queue: List[Request]
    mem_usage: simpy.Container # in MiB
    cpu_use: simpy.Container
    overused_cpu_time_start: int
    overused_cpu_time_end: int
    prev_cpu_use: int
    measuring_interval: bool

    def __init__(self, env, node) -> None:
        super().__init__()
        self.env = env
        self.ether_node = node
        self.instances = dict()
        self.queue = list()
        self.mem_usage = simpy.Container(env, capacity=params.MEMORY_CAPACITY*1024) # MiB
        self.cpu_use = simpy.Container(env)
        self.overused_cpu_time_start = -1
        self.overused_cpu_time_end = -1
        self.prev_cpu_use = 0
        self.measuring_interval = False

    def enqueue(self, request: Request):
        if len(self.queue) < params.QUEUE_LENGTH: # there's space in queue
            self.queue.append(request)
            yield self.env.timeout(params.QUEUE_DELAY)
            ilogger(self.env, request.name, f'Proxy on {self.ether_node.name} queued request={request.request_id}, queue length is {len(self.queue)}')
            self.env.metrics.log_enqueue_time(request, self.ether_node.name, self.env.now)
            self.env.metrics.log_queue_length(request, self.ether_node.name, self.env.now, len(self.queue))
            self.env.process(self.run_scheduler())
        else: # Otherwise, 'drop' request because no space in queue
            wlogger(self.env, request.name, f'WARNING: {self.ether_node.name} queue was full, dropping request={request.request_id}')
            yield self.env.timeout(0)

    def dequeue(self, request: Request):
        assert len(self.queue) > 0
        self.queue.remove(request)
        ilogger(self.env, request.name, f'response sent, Proxy de-queued request={request.request_id}')
        self.env.metrics.log_dequeue_time(request, self.ether_node.name, self.env.now)
        self.env.metrics.log_queue_length(request, self.ether_node.name, self.env.now, len(self.queue))

    def run_scheduler(self):
        self.queue.sort(key=lambda x: x.request_id)
        for request in self.queue:
            if (
                (request.fr_state == FRState.ARRIVED.value) and
                (self.mem_usage.level < (self.ether_node.capacity.memory * 1024)) and
                (len([r for r in self.instances.keys() if r.name == request.name]) < params.MAX_PARALLEL_INSTANCES)
            ):
                request.fr_state = FRState.SCHEDULED.value
                ilogger(self.env, request.name, f'scheduled request={request.request_id}')
                self.env.process(self.invoke(request))
        num_requests = [r for r in self.queue if (r.fr_state == FRState.RUNNING.value)]
        self.env.metrics.log_number_requests_in_flight(self.ether_node.name, self.env.now, len(num_requests))
        yield self.env.timeout(0)

    def execute(self, request: Request):
        ilogger(self.env, request.name, f'finished setting up new Instance for request={request.request_id}')
        ilogger(self.env, request.name, f'executing request={request.request_id} for {request.exec_time} ms, mem usage of {request.mem_use} MiB')
        request.fr_state = FRState.RUNNING.value

        if ((self.mem_usage.level + request.mem_use) > (self.ether_node.capacity.memory * 1024)): # both sides in MiB
            wlogger(self.env, request.name, f'No memory capacity left on host, request={request.request_id} cannot be served')
            yield self.env.timeout(0)
        else:
            yield self.mem_usage.put(request.mem_use)
            yield self.cpu_use.put(request.core_util)
            self.env.metrics.log_cpu_use(request, self.ether_node.name, self.env.now, self.cpu_use.level)
            dlogger(self.env, request.name, f'mem: using {self.mem_usage.level} MiB of RAM on node {self.ether_node.name}')
            dlogger(self.env, request.name, f'cpu: using {self.cpu_use.level} out of {params.NUM_CPU_CORES * 100} on node {self.ether_node.name}')

            if (self.cpu_use.level > (params.NUM_CPU_CORES * 100)):
                if ((not self.measuring_interval) and (self.cpu_use.level != self.prev_cpu_use)):
                    self.overused_cpu_time_start = self.env.now
                    self.measuring_interval = True
                elif (self.measuring_interval and (self.cpu_use.level != self.prev_cpu_use)):
                    self.overused_cpu_time_end = self.env.now
                    self.measuring_interval = False
                    for r in self.queue:
                        if (
                                ((r.start_ts < self.overused_cpu_time_end) and 
                                (r.start_ts >= self.overused_cpu_time_start)) or
                                ((r.end_ts <= self.overused_cpu_time_end) and
                                (r.end_ts > self.overused_cpu_time_start)) or
                                ((r.end_ts == -1) and (r.start_ts != -1))
                            ):
                            interval = self.overused_cpu_time_end - self.overused_cpu_time_start
                            delta = interval*self.cpu_use.level/(params.NUM_CPU_CORES*100)
                            r.delta_exec_time += math.ceil(delta)
                            ilogger(self.env, r.name, f'Request exec time for {r.request_id} is now {r.exec_time} plus {r.delta_exec_time}')
                    self.overused_cpu_time_start = self.env.now
                    self.measuring_interval = True
                    self.overused_cpu_time_end = -1
            else:
                self.overused_cpu_time_start = -1
            self.prev_cpu_use = self.cpu_use.level
            request.start_ts = self.env.now

            yield self.env.timeout(request.exec_time)
            request.end_ts = self.env.now
            yield self.cpu_use.get(request.core_util)
            self.env.metrics.log_cpu_use(request, self.ether_node.name, self.env.now, self.cpu_use.level)
            dlogger(self.env, request.name, f'cpu: using {self.cpu_use.level} out of {params.NUM_CPU_CORES * 100} on node {self.ether_node.name}')
            assert self.cpu_use.level >= 0

            # check cpu use again
            if (self.cpu_use.level > (params.NUM_CPU_CORES * 100)):
                if ((not self.measuring_interval) and (self.cpu_use.level != self.prev_cpu_use)):
                    self.overused_cpu_time_start = self.env.now
                    self.measuring_interval = True
                elif (self.measuring_interval and (self.cpu_use.level != self.prev_cpu_use)):
                    self.overused_cpu_time_end = self.env.now
                    self.measuring_interval = False
                    for r in self.queue:
                        if (
                                ((r.start_ts < self.overused_cpu_time_end) and 
                                (r.start_ts >= self.overused_cpu_time_start)) or
                                ((r.end_ts <= self.overused_cpu_time_end) and
                                (r.end_ts > self.overused_cpu_time_start)) or
                                ((r.end_ts == -1) and (r.start_ts != -1))
                            ):
                            interval = self.overused_cpu_time_end - self.overused_cpu_time_start
                            delta = interval*self.cpu_use.level/(params.NUM_CPU_CORES*100)
                            r.delta_exec_time += math.ceil(delta)
                            ilogger(self.env, r.name, f'Request exec time for {r.request_id} is now {r.exec_time} plus {r.delta_exec_time}')
                    self.overused_cpu_time_start = self.env.now
                    self.overused_cpu_time_end = -1
                    self.measuring_interval = True
            else:
                self.overused_cpu_time_start = -1
            self.prev_cpu_use = self.cpu_use.level

            yield self.env.timeout(request.delta_exec_time)
            self.env.metrics.log_delta_exec(request, self.ether_node.name, request.delta_exec_time)
            yield self.mem_usage.get(request.mem_use)
            dlogger(self.env, request.name, f'mem: using {self.mem_usage.level} MiB of RAM on node {self.ether_node.name}')
            assert self.mem_usage.level >= 0

        self.dequeue(request)
        request.fr_state = FRState.COMPLETE.value
        self.env.process(self.run_scheduler())
        dlogger(self.env, request.name, f'cleaned up request={request.request_id}, {self.mem_usage.level} MiB of RAM in use on node {self.ether_node.name}')

    def invoke(self, request: Request):
        idle_instance = None
        if self.instances:
            for cur_req in self.instances: # Check for idling Instances that can be reused
                if cur_req.name == request.name and self.instances[cur_req].is_idle:
                    idle_instance = self.instances[cur_req]
                    break
        if idle_instance is not None:
            instance = idle_instance
            instance.accept_new_request(request)
            ilogger(self.env, request.name, f'reusing existing idle Instance for request={request.request_id}')
        else:
            instance = Instance(request)
            self.instances[request] = instance
            ilogger(self.env, request.name, f'spinning up new Instance for request={request.request_id}')
            request.fr_state = FRState.SETUP.value
            yield self.env.timeout(params.INSTANCE_SETUP_DELAY)
        yield from self.execute(request)
        yield from instance.idle(self.env)
        if instance.is_idle:
            self.instances.pop(request)
            yield self.env.timeout(params.INSTANCE_CLEANUP_DELAY)
            dlogger(self.env, request.name, f'cleaned up Instance for request={request.request_id} on node {self.ether_node.name}')

    @property
    def name(self):
        return self.ether_node.name

    @property
    def arch(self):
        return self.ether_node.arch

    @property
    def capacity(self) -> Capacity:
        return self.ether_node.capacity


class LoadBalancer:
    def __init__(self, env: Environment) -> None:
        self.env = env
        self.loadbalancercounter = 0

    def loadbalance(self, request: Request):
        chosen_node = self.env.node_states[self.loadbalancercounter]
        self.loadbalancercounter = (self.loadbalancercounter + 1) % len(self.env.node_states)
        ilogger(self.env, request.name, f'load balancer dispatching request={request.request_id} to {chosen_node.name}')
        yield self.env.process(chosen_node.enqueue(request))


class Benchmark():
    def __init__(self, filename: str):
        self.filename = filename
        self.prev_arrival_time = 0

    def receive_request(self, env: Environment, loadbalancer: LoadBalancer, request: Request):
        ilogger(env, request.name, 'new function request arrived at L4 load balancer')
        yield env.process(loadbalancer.loadbalance(request))

    def run(self, env: Environment, loadbalancer: LoadBalancer):
        with open(self.filename, "r") as f:
            reader = csv.reader(f)
            next(reader, None) # skip the headers
            for line in reader:
                app_name = line[0]
                arrival_time = int(line[1])
                execution_time = int(line[2])
                memory_use = int(line[3])
                core_use = int(line[4])
                request = Request(app_name, execution_time, memory_use, core_use)
                start_delay = arrival_time - self.prev_arrival_time
                theproc = start_delayed(env, self.receive_request(env, loadbalancer, request), delay=start_delay)
                self.prev_arrival_time = arrival_time
                yield theproc


class Simulation:
    def __init__(self, topology: Topology, benchmark: Benchmark, env: Environment = None):
        self.env = env or Environment()
        self.topology = topology
        self.benchmark = benchmark

    def run(self):
        logger.info('INIT - initializing simulation, benchmark: %s, topology nodes: %d',
                    type(self.benchmark).__name__, len(self.topology.nodes))
        env = self.env
        self.init_environment(env)
        then = time.time()
        logger.info('INIT - starting benchmark process')
        loadbalancer = LoadBalancer(self.env)
        p = env.process(self.benchmark.run(env, loadbalancer))
        logger.info('INIT - starting simulation')
        env.run(until=p)
        logger.info('Simulation ran %d milliseconds (sim), %.2f seconds (wall)', env.now, (time.time() - then))

    def init_environment(self, env):
        env.benchmark = self.benchmark
        env.topology = self.topology
        nodes = [n for n in env.topology.nodes if isinstance(n, Node)]
        for host in nodes:
            env.node_states.append(NodeState(env, host))
        if not env.metrics:
            env.metrics = Metrics(env, RuntimeLogger())


class Metrics:
    def __init__(self, env: Environment, log: RuntimeLogger = None) -> None:
        super().__init__()
        self.env: Environment = env
        self.logger: RuntimeLogger = log or NullLogger()

    def log(self, metric, value, **tags):
        return self.logger.log(metric, value, **tags)

    def log_enqueue_time(self, request, node_name, enqueue_time, **kwargs):
        self.log(
                'enqueue_timestamp',
                {
                     'request': request.request_id,
                     'node_name': node_name,
                     'enqueue_time': enqueue_time,
                     **kwargs
                }
        )

    def log_dequeue_time(self, request, node_name, dequeue_time, **kwargs):
        self.log(
                'dequeue_timestamp',
                {
                     'request': request.request_id,
                     'node_name': node_name,
                     'dequeue_time': dequeue_time,
                     **kwargs
                }
        )

    def log_delta_exec(self, request, node_name, delta_exec_time, **kwargs):
        self.log(
                'delta_exec',
                {
                    'request': request.name,
                    'node_name': node_name,
                    'delta_exec_time': delta_exec_time,
                    **kwargs
                }
        )

    def log_cpu_use(self, request, node_name, timestamp, cpu_use, **kwargs):
        self.log(
                'cpu_use',
                {
                    'request': request.name,
                    'node_name': node_name,
                    'timestamp': timestamp,
                    'cpu_use': cpu_use,
                    **kwargs
                }
        )

    def log_queue_length(self, request, node_name, timestamp, queuelength, **kwargs):
        self.log(
                'queuelength', 
                {
                    'request': request.name, 
                    'node_name': node_name, 
                    'timestamp': timestamp, 
                    'queuelength': queuelength, 
                    **kwargs
                }
        )

    def log_number_requests_in_flight(self, node_name, timestamp, num_reqs, **kwargs):
        self.log(
                'number_requests_in_flight',
                {
                    'node_name': node_name,
                    'timestamp': timestamp,
                    'num_reqs': num_reqs,
                    **kwargs
                }
        )

    def get(self, name, **tags):
        return self.logger.get(name, **tags)

    @property
    def clock(self):
        return self.clock

    @property
    def records(self):
        return self.logger.records

    def extract_dataframe(self, measurement: str):
        data = list()

        for record in self.records:
            if record.measurement != measurement:
                continue

            r = dict()
            r['time'] = record.time
            for k, v in record.fields.items():
                r[k] = v
            for k, v in record.tags.items():
                r[k] = v

            data.append(r)
        df = pd.DataFrame(data)

        if len(data) == 0:
            return df

        df.index = pd.DatetimeIndex(pd.to_datetime(df['time']))
        del df['time']
        return df


if __name__ == '__main__':
    main(sys.argv[1])
