#!/bin/bash

cd ~/point-to-point/tcpTest
hostname=`hostname`
ls
rm ~/point-to-point/tcpTest/tcpTest
##rm *.dat
go build 
./tcpTest $hostname hosts.txt
