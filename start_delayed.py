# SPDX-License-Identifier: MIT
# Copyright (C) 2013 Ontje Lünsdorf and Stefan Scherfke
# Copyright (C) 2025 Natalie Balashov

from simpy import *


def start_delayed(env, generator, delay):
    if delay < 0:
        raise ValueError(f'delay(={delay}) must be >= 0.')

    def starter():
        yield env.timeout(delay)
        proc = env.process(generator)
        return proc

    return env.process(starter())
