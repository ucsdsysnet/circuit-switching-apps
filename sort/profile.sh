#!/bin/bash

host=`hostname`
sample_interval=1
number_of_samples=350

fortyG=40
tenG=10

reactor="reactor"
b09="b09"
#speed=10
speed=$tenG
speed=$fortyG

rack=$reactor
rack=$b09

#interface=ens1f0
interface=ens2d1
#echo "" > test.agg

#network_interface=ens1f0
network_interface=ens2d1

if [[ $rack -eq $reactor ]]; then
    if [[ $speed -eq $fortyG ]];then
        case $host in
        "reactor3" | "reactor4" | "reactor5" | "reactor7")
            interface=ens2f1
            ;;
        *)
            interface=ens2d1
            ;;
        esac
    elif [[ $speed -eq $tenG ]]; then
        interface=ens1f0
    else
        echo "Speed and host unknown exiting"
    fi
elif [[ $rack -eq $b09 ]]; then
    interface=enp101s0
else
    echo "rack $rack unknown"
fi

sar -r $sample_interval $number_of_samples > data/${host}_ram.dat &
# Swap
sar -u $sample_interval $number_of_samples > data/${host}_cpu.dat &
# RAM
sar -S $sample_interval $number_of_samples  > data/${host}_swap.dat &
# Load average and tasks
sar -q $sample_interval $number_of_samples  > data/${host}_loadaverage.dat &
# IO transfer
sar -b $sample_interval $number_of_samples  > data/${host}_iotransfer.dat &
# Process/context switches
sar -w $sample_interval $number_of_samples > data/${host}_proc.dat &
# Network Interface
sar -n DEV $sample_interval $number_of_samples > data/${host}_netinterface.dat &
# Sockets
sar -n SOCK $sample_interval $number_of_samples  > data/${host}_sockets.dat &
# Power

