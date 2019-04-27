#!/bin/bash

rdir=~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort
#latencyfiles=../data/*bw.dat
#latencyfiles=../data/*Bandwidth.dat
#python latency.py $latencyfiles

#sarfiles=../data/*.agg
sarfiles=$rdir/data/b09*edu.agg
python $rdir/plot/sar.py $sarfiles
