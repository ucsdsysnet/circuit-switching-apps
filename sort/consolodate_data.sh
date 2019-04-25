#!/bin/bash

fortyG=40
tenG=10
#speed=10
speed=$tenG
speed=$fortyG


reactor="reactor"
b09="b09"

rack=$reactor
rack=$b09


#interface=ens1f0
interface=ens2d1
#echo "" > test.agg
cd data
file=../hosts.txt
while read LINE; do
    host=`echo $LINE | cut -d ' ' -f 1`
    #fix interface names
    #Reactor7 is using a different intel nic than 6 & 8 April 16 2019

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
    
    echo $host $interface

    echo "" > ${host}.agg
    cat ${host}_cpu.dat | tail -n +4 | head -n -1 |  awk '{print $1}' | paste ${host}.agg - | sponge ${host}.agg
    cat ${host}_cpu.dat | tail -n +4 | head -n -1 | awk '{print $4}' | paste ${host}.agg - | sponge ${host}.agg

        
    cat ${host}_netinterface.dat | grep $interface |  head -n -1 | awk '{print $7}' | paste ${host}.agg - | sponge ${host}.agg
    cat ${host}_netinterface.dat | grep $interface |  head -n -1 | awk '{print $6}' | paste ${host}.agg - | sponge ${host}.agg
    cat ${host}_ram.dat | tail -n +4|  head -n -1 | awk '{print $6}' | paste ${host}.agg - | sponge ${host}.agg
    cat ${host}_iotransfer.dat | tail -n +4|  head -n -1 | awk '{print $6}' | paste ${host}.agg - | sponge ${host}.agg

    sed -i '1s/^/time,cpu,rec,send,ram,disk\n/' ${host}.agg
    sed -i 's/\t/,/g' ${host}.agg
    sed -i 's/^,//g' ${host}.agg
    sed -i '/,,,/d' ${host}.agg
done < $file

