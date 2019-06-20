import numpy as np
from pylab import *
from matplotlib.pyplot import figure
import matplotlib.pyplot as plt
import csv
import sys

powerMeasureTimeDiff=0

def get_sec(time_str):
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)

def hmsToSec(time):
    convTime = []
    for val in time:
        convTime.append(get_sec(val))
    return convTime

def per(p, x, y):
    absval = np.percentile(y, p)
    px = []
    py = []
    for i in xrange(len(y)):
        if y[i] > absval:
            px.append(x[i])
            py.append(y[i])
    return px, py, absval

def normalizeTime(time):
    minimum = min(time)
    i=0
    for val in time:
        time[i] = val-minimum
        i=i+1
    return time

def normalizeTime5(time):
    minimum = min(time)
    i=0
    for val in time:
        time[i] = val-minimum + powerMeasureTimeDiff
        i=i+1
    return time

def KbpsToGbps(tput):
    i = 0
    for val in tput:
        tput[i] = (val  * 8) / 1000000.0
        i = i+1
    #print lat
    return tput

def findMins(chart):
    threshold = 15
    gap = 5
    current = chart[0]
    tail = chart[0]
    inflection = []
    i = 0
    for val in chart:
        if i > gap and i < len(chart) :
            current = chart[i]
            tail = chart[i-gap]
            diff=tail - current
            if diff > threshold:
                while chart[i] < chart[i-1]:
                    i= i + 1
                inflection.append(i)
                i=i+10
        i=i+1

    return inflection
        





#ax = plt.gca()
#ax.get_xaxis().get_major_formatter().set_scientific(False)


sys.argv.pop(0)
speed=sys.argv.pop(0)
#print str(sys.argv)

print "plotting power for ", speed

plt.rcParams.update({'font.size': 24})

#plot the latencies of each individual measure
color=iter(cm.rainbow(np.linspace(0,1,5*(len(sys.argv) + 1))))
c=next(color)


globalXLim = 160
globalXMin = -1


networkingYmax = 42
############################################################################
#                       CPU
############################################################################
time = []
cpupower = []
mempower = []
cpupower2 = []
mempower2 = []
totalpower = []

total = []

timeindex=0
cpupowerindex=1
mempowerindex=2
cpupowerindex2=3
mempowerindex2=4


epochs=[0,0,0]
bpower=[[] for y in range(4)]
raplcpupower=[[] for y in range(8)]
raplmempower=[[] for y in range(8)]
diffpower=[[] for y in range(4)]
monitertime=[]

color = ['b','r','c','k', 'm', 'g', 'y', '#008000']
color = [
    '#e6194B'
    ,'#f58231'
    ,'#bfef45'
    ,'#3cb44b'
    ,'#42d4f4'
    ,'#4363d8'
    ,'#911eb4'
    ,'#000000']
linetype = ['g-','g--','g-+','g-.']
bhostlist = ["b09-30","b09-32","b09-34", "b09-36"]
cindex=0
lindex=1

#parameters
figx=32
figy=20
dpi=80
figure(num=None, figsize=(figx, figy), dpi=dpi, facecolor='w', edgecolor='k')
xlabel="Seconds"
ylabel="Energy Draw (Joules)"
title="800GB Sort 8 Numa Hosts Power Usage"

#xlegendanchor=0.50
#ylegendanchor=-0.84


xlegendanchor=0.50
ylegendanchor=-0.35


xlimleft = globalXMin
xlimright = globalXLim
ylimlower = -3
ylimupper = 475


tlayx0=0.02
tlayy0=0.24
tlayx1=1
tlayy1=1

chartname="Rapl_CPU_Mem_Power.png"

hostindex=0
lindex=0
# calculate 99th percentile
for filename in sys.argv:
    if "shuTingPower" in filename:
        with open(filename,'r') as csvfile:
            plots = csv.reader(csvfile, delimiter=',')
            cindex=0
            bhindex=0
            lindex=3
            for row in plots:
                monitertime.append(int(float(row[timeindex])))
                bpower[0].append(float(row[1]))
                bpower[1].append(float(row[2]))
                bpower[2].append(float(row[3]))
                bpower[3].append(float(row[4]))

                diffpower[0].append(float(row[1]))
                diffpower[1].append(float(row[2]))
                diffpower[2].append(float(row[3]))
                diffpower[3].append(float(row[4]))

            monitertime = normalizeTime5(monitertime)
        
            time = []
            ntime = []
            cindex=0
            lindex=0
            #print "done plotting agg"
            continue

    with open(filename,'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')

    
        sname = filename.split("/")
        subname = sname[len(sname)-1]
        leftname = subname.split(".")
        finalname = leftname[0]
       # print(finalname)

        i=0
        for row in plots:
                time.append(int(row[timeindex]))
                cpupower.append(float(row[cpupowerindex]))
                mempower.append(float(row[mempowerindex]))
                cpupower2.append(float(row[cpupowerindex2]))
                mempower2.append(float(row[mempowerindex2]))
                if hostindex < len(diffpower):
                    if i > powerMeasureTimeDiff and hostindex < len(diffpower) and (i-powerMeasureTimeDiff) < len(diffpower[hostindex]):
                        diffpower[hostindex][i - powerMeasureTimeDiff] -= (float(row[cpupowerindex]) + float(row[mempowerindex]))
                i = i + 1

        ntime = normalizeTime(time)


        mins = findMins(cpupower)
        t=0
        if len(mins) != 3:
            #print(mins)
            #print("NOT ENOUGH MINIMA - stealing from another host")
            for val in epochs:
                #print("TEST")
                epochs[t] += float(val) / float(hostindex)
                t=t+1
        else:
            for val in mins:
                epochs[t] += val
                t=t+1
        print epochs


        plt.plot(ntime, cpupower, linetype[lindex], label=finalname+"_cpu_1" , color=color[cindex], linewidth=5)
        plt.plot(ntime, mempower, linetype[lindex+1], label=finalname+"_mem_1" , color=color[cindex], linewidth=5)
        plt.plot(ntime, cpupower2, linetype[lindex+2], label=finalname+"_cpu_2" , color=color[cindex], linewidth=5)
        plt.plot(ntime, mempower2, linetype[lindex+3], label=finalname+"_mem_2" , color=color[cindex], linewidth=5)
        
        if hostindex < len(diffpower):
            if finalname=="b09-36":
                finalname="Barefoot_Switch"
                plt.plot(monitertime, bpower[hostindex], 'g^', label=finalname+"_PM" , color=color[cindex], linewidth=5)
            else:
                plt.plot(monitertime, bpower[hostindex], linetype[lindex+3], label=finalname+"_PM" , color=color[cindex], linewidth=5)

        
        #raplcpupower[hostindex] = cpupower
        #raplmempower[hostindex] = mempower

        raplcpupower[hostindex] = [x + y for x, y in zip(cpupower, cpupower2)]
        raplmempower[hostindex] = [x + y for x, y in zip(mempower, mempower2)]


        cindex=cindex+1
        hostindex=hostindex+1
        time =[]
        cpupower = []
        mempower = []
        cpupower2 = []
        mempower2 = []

j=0
for val in epochs:
    corrected=int(float(val)/float(hostindex))
    plt.axvline(x=corrected,linewidth=10)
    epochs[j]=corrected
    j=j+1

lgd = plt.legend(ncol=6,loc="lower center",bbox_to_anchor=(xlegendanchor,ylegendanchor))
plt.grid('on')
plt.tight_layout(rect=(tlayx0,tlayy0,tlayx1,tlayy1))
plt.title(title)
plt.xlabel(xlabel)
plt.ylabel(ylabel)

plt.xlim(left=xlimleft,right=xlimright)
plt.ylim(bottom=ylimlower,top=ylimupper)
plt.savefig(chartname, format='png')
#plt.show()
plt.clf()





figx=30
figy=10
dpi=80
figure(num=None, figsize=(figx, figy), dpi=dpi, facecolor='w', edgecolor='k')

title="Power Meter, Rapl (cpu mem) Difference"
ylabel="Energy Difference (Joules)"

ylimlower = -50
ylimupper = 350


tlayx0=0.03
tlayy0=0.15
tlayx1=1
tlayy1=1


xlegendanchor=0.50
ylegendanchor=-0.47

chartname="power_diff.png"
i=0
cindex=0
for dp in diffpower:
    if i < len(bhostlist):
        plt.plot(monitertime, dp, linetype[0],label=bhostlist[i],color=color[cindex], linewidth = 5)
    cindex += 1
    i += 1
    

lgd = plt.legend(ncol=4,loc="lower center",bbox_to_anchor=(xlegendanchor,ylegendanchor))
plt.grid('on')
plt.tight_layout(rect=(tlayx0,tlayy0,tlayx1,tlayy1))
plt.xlim(left=xlimleft,right=xlimright)
plt.ylim(bottom=ylimlower,top=ylimupper)
plt.title(title)
plt.xlabel(xlabel)
plt.ylabel(ylabel)
plt.savefig(chartname, format='png')

#calculate sum of total power
fourty = "40"
twentyfive = "25"
ten = "10"

#THIS IS THE IMPORTANT VAR
#speed = 10

powerreadstart=epochs[0]
shufflePowerRead=epochs[1]
fullPowerRead=epochs[2]

print epochs


spsum= []
fpsum= []

scpusum = []
smemsum = []

fcpusum = []
fmemsum = []


h=0
for host in bpower:
    i=0
    spsum.append(0)
    fpsum.append(0)
    scpusum.append(0)
    smemsum.append(0)
    fcpusum.append(0)
    fmemsum.append(0)
    #if h >= 3:
    #    break
    for val in host:
        if i > powerreadstart and i < shufflePowerRead :
            spsum[h] += bpower[h][i]
            scpusum[h] += raplcpupower[h][i]
            smemsum[h] += raplmempower[h][i]
        if i > powerreadstart and i < fullPowerRead :
            fpsum[h] += bpower[h][i]
            fcpusum[h] += raplcpupower[h][i]
            fmemsum[h] += raplmempower[h][i]
        i = i + 1
    h = h + 1

#print "Speed " ,speed
#print "Full Power ", fpsum, " Joules"
#print "Shuffle Power ", spsum, " Joules"
#print "Sort Power ", fpsum - spsum, " Joules"
output = ( str(speed) + ',' +
           str(scpusum[0]) + ',' + str(smemsum[0]) + ',' + str(spsum[0]) + ',' +
           str(scpusum[1]) + ',' + str(smemsum[1]) + ',' + str(spsum[1]) + ',' +
           str(scpusum[2]) + ',' + str(smemsum[2]) + ',' + str(spsum[2]) + ',' +
           str(scpusum[3]) + ',' + str(smemsum[3]) + ',' + str(spsum[3]) + ',' +
           str(fcpusum[0]) + ',' + str(fmemsum[0]) + ',' + str(fpsum[0]) + ',' +
           str(fcpusum[1]) + ',' + str(fmemsum[1]) + ',' + str(fpsum[1]) + ',' +
           str(fcpusum[2]) + ',' + str(fmemsum[2]) + ',' + str(fpsum[2]) + ',' +
           str(fcpusum[3]) + ',' + str(fmemsum[3]) + ',' + str(fpsum[3]) + ',' )
print output

