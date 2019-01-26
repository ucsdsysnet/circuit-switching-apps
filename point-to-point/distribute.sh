#!/bin/bash

IFS=$'\n'       # make newlines the only separator
#set -f          # disable globbing
for i in $(cat < hosts.txt); do
  hostname=`echo $i | cut -d ' ' -f 1`
  IFS=$tmp
  scp -r * $hostname:~/point-to-point
  exit 0 #there is no need to loop the server replicate everything. Remember to name files well!
done
