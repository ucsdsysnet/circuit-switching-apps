#!/bin/bash

IFS=$'\n'       # make newlines the only separator
#set -f          # disable globbing
for i in $(cat < hosts.txt); do
  hostname=`echo $i | cut -d ' ' -f 1`
  IFS=$tmp
  scp -r *.go ssgrant@$hostname:~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/ &
  scp -r ./lib/*.go ssgrant@$hostname:~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/lib &
  scp -r ./*.sh ssgrant@$hostname:~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/ &
  scp -r ./hosts.txt ssgrant@$hostname:~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/ &
  #scp -r * ssgrant@$hostname:~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/
done

sleep 2
