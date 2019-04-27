#!/bin/bash -x

source ~/.bashrc
source /home/ssgrant/.bashrc
cd ~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort
pwd
hostname=`hostname`
ls
./profile.sh &
echo "Echoing PATH!!"
echo $PATH

nohup sudo ~/sort_power/build/rapl_measure 100 >  output.log 2>&1 &
sleep 5
#wakeup and run hostname

if [[ $hostname == "b09-30.sysnet.ucsd.edu" ]]; then
    python3 ~/sort_power/meter_reading.py shuTingPower 100 1 &
fi
go run main.go $hostname hosts.txt
#rm ~/point-to-point/point-to-point
#go build 
#./point-to-point $hostname hosts.txt
