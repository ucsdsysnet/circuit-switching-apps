#!/bin/bash

IFS=$'\n'        # make newlines the only separator
#set -f          # disable globbing
for i in $(cat < hosts.txt); do
  hostname=`echo $i | cut -d ' ' -f 1`
  echo $hostname
  #TODO In the future set this with options
  ssh ssgrant@$hostname "killall main; killall profile.sh; killall profile; killall sar; killall rapl_measure; killall python3; killall sadc; killall sar; killall sort; killall hadoop" &
done
  sleep 2
