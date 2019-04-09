#!/bin/bash

hname=`hostname`
sample_interval=1
number_of_samples=300
network_interface=ens1f0


sar -r $sample_interval $number_of_samples > data/${hname}_ram.dat &
# Swap
sar -u $sample_interval $number_of_samples > data/${hname}_cpu.dat &
# RAM
sar -S $sample_interval $number_of_samples  > data/${hname}_swap.dat &
# Load average and tasks
sar -q $sample_interval $number_of_samples  > data/${hname}_loadaverage.dat &
# IO transfer
sar -b $sample_interval $number_of_samples  > data/${hname}_iotransfer.dat &
# Process/context switches
sar -w $sample_interval $number_of_samples > data/${hname}_proc.dat &
# Network Interface
sar -n DEV $sample_interval $number_of_samples > data/${hname}_netinterface.dat &
# Sockets
sar -n SOCK $sample_interval $number_of_samples  > data/${hname}_sockets.dat &
