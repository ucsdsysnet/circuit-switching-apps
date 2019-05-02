#!/bin/bash

rdir=~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort
#latencyfiles=../data/*bw.dat
#latencyfiles=../data/*Bandwidth.dat
#python latency.py $latencyfiles

sarfiles=../data/*.agg
sarfiles=$rdir/data/b09*edu.agg
python $rdir/plot/sar.py $sarfiles

powfiles=$rdir/data/*rapl.dat
aggpowfile=$rdir/data/power_shuTingPower.dat

python $rdir/plot/pow.py $aggpowfile $powfiles
