#!/bin/bash

speed=$1

rdir=~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort
datadir=~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/data

sarfiles=`cut -d ' ' -f 1 $rdir/hosts.txt | awk -v pre=$rdir/data/ -v suf=.agg '{print pre$0suf}'`

echo " "
echo $sarfiles
echo " "


#sarfiles=$rdir/data/b09*edu.agg
python $rdir/plot/sar.py $sarfiles

powfiles=`cut -d ' ' -f 1 $rdir/hosts.txt | awk -v pre=$rdir/data/ -v suf=_rapl.dat '{print pre$0suf}'`

echo " "
#powfiles=$rdir/data/*rapl.dat
aggpowfile=$rdir/data/shuTingPower.txt
timerfile=$rdir/data/timer.dat


#python $rdir/ploy/aggpow.py powerstats.dat

powerfile=powerstats-full-mem.dat
ut=`date +%s`
python $rdir/plot/pow.py $speed $aggpowfile $powfiles >> $powerfile
#python $rdir/plot/pow.py $speed $aggpowfile $powfiles

t=`tail -1 $timerfile`
t2=`tail -1 $powerfile`

runsummary="runsummary.dat"

sed -i '$ d' $powerfile
echo "$t2$t" >> $powerfile
echo "$t2$t" > $runsummary


#sed -i 's,$,'"$t"',' "$powerfile"; cat $powerfile
#echo Time $t

#save data
mkdir $datadir/$ut-$speed
cp $powfiles $datadir/$ut-$speed
cp $aggpowfile $datadir/$ut-$speed
cp $sarfiles $datadir/$ut-$speed
cp $timerfile $datadir/$ut-$speed
cp $runsummary $datadir/$ut-$speed
cp *.png $datadir/$ut-$speed

ln -s $datadir/$ut-$speed latest_data
