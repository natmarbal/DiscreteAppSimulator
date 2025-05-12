Discrete App Simulator
=====================

Discrete App Simulator is a serverless simulator inspired by [faas-sim](https://github.com/edgerun/faas-sim).

Running a simulation
--------------------

In the root directory, run the following commands:
```bash
    python3.8 -m venv ./venv
    source ./venv/bin/activate
    pip install -Ur requirements.txt
    python main.py workload.csv
```
which outputs an `out.log` file and various `df_*.csv` files with logs about the simulator run.

The provided `workload.csv` file was generated from the
[2021 Azure data trace](https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsInvocationTrace2021.md)
using the script `get_subset_azure.py`.
Any CSV file with the required fields may be inputted to the simulator instead.

[This paper](https://open.library.ubc.ca/soa/cIRcle/collections/undergraduateresearch/52966/items/1.0448503)
describes the simulator in detail.
