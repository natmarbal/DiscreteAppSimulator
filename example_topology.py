# SPDX-License-Identifier: MIT
# Copyright (C) 2025 Natalie Balashov

import params
import matplotlib.pyplot as plt
import networkx as nx
from ether.topology import Topology
from ether.vis import draw_basic
from ether.blocks.nodes import create_node
from ether.cell import LANCell
from ether.blocks.cells import BusinessIsp
from ether.core import Connection

class ExampleScenario:
    def __init__(self, num_init_servs=1) -> None:
        super().__init__()
        self.num_init_servs = num_init_servs

    def materialize(self, topology: Topology):
        nodes = []
        for i in range(self.num_init_servs):
            servname = f'server_{i}'
            serv = create_node(name=servname, cpus=params.NUM_CPU_CORES, arch='x86', mem=str(params.MEMORY_CAPACITY)+'G', labels={'type': 'server'})
            nodes.append(serv)
        cell = LANCell(nodes, backhaul=BusinessIsp())
        cell.materialize(topology)


if __name__ == '__main__':
    T = Topology()
    ExampleScenario().materialize(T)

    draw_basic(T)
    fig = plt.gcf()
    fig.set_size_inches(2, 1)
    plt.show()
