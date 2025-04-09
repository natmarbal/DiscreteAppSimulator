# ----------------- #
# SYSTEM PARAMETERS #
# ----------------- #

NUM_CPU_CORES = 8 # per node
MEMORY_CAPACITY = 32 # per node, in GiB

INSTANCE_SETUP_DELAY = 10 # milliseconds
INSTANCE_CLEANUP_DELAY = 10 # milliseconds
INSTANCE_IDLE_TIME = 20 # amount of time an Instance is kept around before being clean up, in milliseconds

QUEUE_LENGTH = 100 # physical limit of scheduler queue, number of requests that can queue at any given point in time
QUEUE_DELAY = 0 # Delay for enqueuing a request

MAX_PARALLEL_INSTANCES = NUM_CPU_CORES / 2 # max number of parallel Instances that a single node can run for any single app
