#!/bin/bash -x

cd ~/point-to-point
pwd
hostname=`hostname`
ls

go run main.go $hostname hosts.txt
#rm ~/point-to-point/point-to-point
#go build 
#./point-to-point $hostname hosts.txt
