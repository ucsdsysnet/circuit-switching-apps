#!/bin/bash

speed=$1

echo "Running sort at $speed Gbps"

measuretimerapl=150
measuretimePU=150



source ~/.bashrc
source /home/ssgrant/.bashrc
cd ~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort


if [[ $hostname == "b09-30.sysnet.ucsd.edu" ]]; then
    rm data/*.dat
fi

cd lib
go build
cd ..

pwd
hostname=`hostname`
ls
./profile.sh &
echo "Echoing PATH!!"
echo $PATH

d=`date +%s`

sudo modprobe msr
nohup sudo ~/sort_power/build/rapl_measure "rapl_$d" $measuretimerapl >  output.log 2>&1 &
#wakeup and run hostname

if [[ $hostname == "b09-30.sysnet.ucsd.edu" ]]; then
    echo starting power reader
    python3 ~/sort_power/meter_reading.py shuTingPower $measuretimePU 1 &
fi

numactl --preferred=1 go run main.go $hostname hosts.txt $speed
#numactl --membind=1 --cpubind=1 go run main.go $hostname hosts.txt $speed

#go run main.go $hostname hosts.txt $speed
#rm ~/point-to-point/point-to-point
#go build 
#./point-to-point $hostname hosts.txt
