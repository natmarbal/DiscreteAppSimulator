# SPDX-License-Identifier: MIT
# Copyright (C) 2025 Natalie Balashov

import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv('AzureFunctionsInvocationTraceForTwoWeeksJan2021.txt') # Assumes Azure trace is downloaded locally
df['execution_time'] = np.ceil(df['duration']).astype(int)
df = df[df['execution_time'] > 0]
df['arrival_time'] = np.ceil(df['end_timestamp']).astype(int) - df['execution_time']
np.random.seed(42)
df['memory_use'] = np.random.randint(1, 501, df.shape[0])
df['core_utilization'] = np.random.randint(1, 101, df.shape[0])
df = df.drop('app', axis=1)
df = df.drop('func', axis=1)
df = df.drop('end_timestamp', axis=1)
df = df.drop('duration', axis=1)
df = df.sort_values("arrival_time")
df['app_name'] = np.arange(len(df))
df = df[['app_name', 'arrival_time', 'execution_time', 'memory_use', 'core_utilization']]

azure = df.head(500)
azure.to_csv(f'workload.csv', sep=',', index=False)
print("Wrote subset of modified Azure trace to workload.csv")
