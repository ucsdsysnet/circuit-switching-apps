#!/bin/bash

hname=`hostname`

echo $hname

case $hname in

"reactor1")
    sudo ifconfig ens1f0 192.168.1.101 netmask 255.255.255.0
    ;;
"reactor2")
    sudo ifconfig ens1f0 192.168.1.102 netmask 255.255.255.0
    ;;
"reactor3")
    sudo ifconfig ens1f0 192.168.1.103 netmask 255.255.255.0
    ;;
"reactor4")
    sudo ifconfig ens1f0 192.168.1.104 netmask 255.255.255.0
    ;;
"reactor5")
    sudo ifconfig ens1f0 192.168.1.105 netmask 255.255.255.0
    sudo ifconfig ens2f1 172.16.1.105 netmask 255.255.255.0
    ;;
"reactor6")
    sudo ifconfig ens1f0 192.168.1.106 netmask 255.255.255.0
    sudo ifconfig ens2d1 172.16.1.106 netmask 255.255.255.0
    ;;
"reactor7")
    sudo ifconfig ens1f0 192.168.1.107 netmask 255.255.255.0
    sudo ifconfig ens2f1 172.16.1.107 netmask 255.255.255.0
    ;;
"reactor8")
    sudo ifconfig ens1f0 192.168.1.108 netmask 255.255.255.0
    sudo ifconfig ens2d1 172.16.1.108 netmask 255.255.255.0
    ;;
"reactor9")
    sudo ifconfig ens1f0 192.168.1.109 netmask 255.255.255.0
    ;;
*)
    echo "hostname >>> " $hname " <<< unknown to configuration script"
esac
