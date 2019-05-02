#!/bin/bash

measuretimerapl=130
measuretimePU=130

source ~/.bashrc
source /home/ssgrant/.bashrc
cd ~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort
pwd
hostname=`hostname`
ls
./profile.sh &
echo "Echoing PATH!!"
echo $PATH

sudo modprobe msr
nohup sudo ~/sort_power/build/rapl_measure $measuretimerapl >  output.log 2>&1 &
#wakeup and run hostname

if [[ $hostname == "b09-30.sysnet.ucsd.edu" ]]; then
    python3 ~/sort_power/meter_reading.py shuTingPower $measuretimePU 1 &
fi
sleep 5
go run main.go $hostname hosts.txt
#rm ~/point-to-point/point-to-point
#go build 
#./point-to-point $hostname hosts.txt
