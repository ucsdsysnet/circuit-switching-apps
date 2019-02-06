#!/bin/bash

#latencyfiles=../data/*bw.dat
latencyfiles=../data/*Bandwidth.dat

python latency.py $latencyfiles
