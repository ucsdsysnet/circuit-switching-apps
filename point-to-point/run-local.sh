#!/bin/bash

cd ~/point-to-point
hostname=`hostname`
go run main.go $hostname hosts.txt
