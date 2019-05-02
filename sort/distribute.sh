#!/bin/bash

IFS=$'\n'       # make newlines the only separator
#set -f          # disable globbing
for i in $(cat < hosts.txt); do
  hostname=`echo $i | cut -d ' ' -f 1`
  IFS=$tmp
  scp -r ./*.go $hostname:~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/ &
  scp -r ./*.sh $hostname:~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/ &
done

sleep 1
