#!/bin/bash
fname=`ls /tmp/rapl_[1-9]*.txt | sort | tail -1`
hname=`hostname`
echo $fname
echo $hname
cp $fname "/home/ssgrant/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/data/${hname}_rapl.dat"
