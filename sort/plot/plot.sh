#!/bin/bash

rdir=~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort
datadir=~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/data

sarfiles=$rdir/data/b09*edu.agg
python $rdir/plot/sar.py $sarfiles

powfiles=$rdir/data/*rapl.dat
aggpowfile=$rdir/data/power_shuTingPower.dat

python $rdir/plot/pow.py $aggpowfile $powfiles

ut=`date +%s`
mkdir $datadir/$ut

#save data
cp $powfiles $datadir/$ut
cp $aggpowfile $datadir/$ut
cp $sarfiles $datadir/$ut
cp *.png $datadir/$ut
