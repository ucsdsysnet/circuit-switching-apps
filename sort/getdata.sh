
IFS=$'\n'       # make newlines the only separator
#set -f          # disable globbing
for i in $(cat < hosts.txt); do
  hostname=`echo $i | cut -d ' ' -f 1`
  IFS=$tmp
  ssh -x ssgrant@$hostname "/home/ssgrant/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/latest_rapl.sh; killall sar"
done

for i in $(cat < hosts.txt); do
  hostname=`echo $i | cut -d ' ' -f 1`
  IFS=$tmp
  scp ssgrant@$hostname:~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/data/*.dat ./data/
  scp b09-30:~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/data/*.dat ./data/
  scp b09-30:~/go/src/github.com/wantonsolutions/circuit-switch-apps/sort/shuTingPower.txt ./data/
  exit 0 #there is no need to loop the server replicate everything. Remember to name files well!
done

