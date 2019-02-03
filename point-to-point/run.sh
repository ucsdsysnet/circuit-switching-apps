#!/bin/bash

IFS=$'\n'       # make newlines the only separator
#set -f          # disable globbing
for i in $(cat < hosts.txt); do
  hostname=`echo $i | cut -d ' ' -f 1`
  echo $hostname
  ssh $hostname $1 &
done

sleep 9999
