#!/bin/bash

SPEED=$1

IFS=$'\n'        # make newlines the only separator
#set -f          # disable globbing
for i in $(cat < hosts.txt); do
  hostname=`echo $i | cut -d ' ' -f 1`
  echo $hostname
  #TODO In the future set this with options
  ssh ssgrant@$hostname "~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/run-sort.sh $SPEED" &
  #sleep 1
done

