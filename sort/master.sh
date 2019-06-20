#!/bin/bash

speed=$1

./kill.sh
./distribute.sh
./run.sh $speed
sleep 160
./getdata.sh
./consolodate_data.sh
./plot/plot.sh $speed
