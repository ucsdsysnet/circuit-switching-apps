#!/bin/bash

cd ~/point-to-point
hostname=`hostname`
ls
rm ~/point-to-point/point-to-point
go build 
./point-to-point $hostname hosts.txt
