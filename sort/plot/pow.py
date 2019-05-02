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





#ax = plt.gca()
#ax.get_xaxis().get_major_formatter().set_scientific(False)


sys.argv.pop(0)
#print str(sys.argv)

plt.rcParams.update({'font.size': 42})

#plot the latencies of each individual measure
color=iter(cm.rainbow(np.linspace(0,1,5*(len(sys.argv) + 1))))
c=next(color)


globalXLim = 130
globalXMin = -1


networkingYmax = 42
############################################################################
#                       CPU
############################################################################
time = []
cpupower = []
mempower = []
totalpower = []

total = []

timeindex=0
cpupowerindex=1
mempowerindex=2

b30index=1
b32index=2
b34index=3
b36index=4

bpower=[[] for y in range(4)]
diffpower=[[] for y in range(4)]
monitertime=[]

print bpower
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
figy=16
dpi=80
figure(num=None, figsize=(figx, figy), dpi=dpi, facecolor='w', edgecolor='k')
xlabel="Seconds"
ylabel="Energy Draw (Joules)"
title="228GB Sort 8 Hosts Power Usage (4 monitered)"

#xlegendanchor=0.50
#ylegendanchor=-0.84


xlegendanchor=0.50
ylegendanchor=-0.50


xlimleft = globalXMin
xlimright = globalXLim
ylimlower = -3
ylimupper = 260


tlayx0=0.02
tlayy0=0.24
tlayx1=1
tlayy1=1

chartname="Rapl_CPU_Mem_Power.png"

hostindex=0
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
        
            #plt.plot(ntime, b30power, linetype[lindex], label=bhostlist[bhindex]+"_PM", color=color[cindex], linewidth=5)
            #cindex = cindex + 1
            #bhindex = bhindex + 1
            #plt.plot(ntime, b32power, linetype[lindex], label=bhostlist[bhindex]+"_PM", color=color[cindex], linewidth=5)
            #cindex = cindex + 1
            #bhindex = bhindex + 1
            #plt.plot(ntime, b34power, linetype[lindex], label=bhostlist[bhindex]+"_PM", color=color[cindex], linewidth=5)
            #cindex = cindex + 1
            #bhindex = bhindex + 1
            #plt.plot(ntime, b36power, linetype[lindex], label=bhostlist[bhindex]+"_PM", color=color[cindex], linewidth=5)
            #cindex = cindex + 1
            #bhindex = bhindex + 1


            time = []
            ntime = []
            cindex=0
            lindex=0
            print "done plotting agg"
            continue

    with open(filename,'r') as csvfile:
        if hostindex >= len(diffpower):
            continue
        plots = csv.reader(csvfile, delimiter=',')

    
        sname = filename.split("/")
        subname = sname[len(sname)-1]
        leftname = subname.split(".")
        finalname = leftname[0]
        print(finalname)

        i=0
        for row in plots:
                time.append(int(row[timeindex]))
                cpupower.append(float(row[cpupowerindex]))
                mempower.append(float(row[mempowerindex]))
                if i > powerMeasureTimeDiff and hostindex < len(diffpower) and (i-powerMeasureTimeDiff) < len(diffpower[hostindex]):
                    diffpower[hostindex][i - powerMeasureTimeDiff] -= (float(row[cpupowerindex]) + float(row[mempowerindex]))
                i = i + 1

        ntime = normalizeTime(time)

        plt.plot(ntime, cpupower, linetype[lindex], label=finalname+"_cpu" , color=color[cindex], linewidth=5)
        plt.plot(ntime, mempower, linetype[lindex+1], label=finalname+"_mem" , color=color[cindex], linewidth=5)
        plt.plot(monitertime, bpower[hostindex], linetype[lindex+3], label=finalname+"_PM" , color=color[cindex], linewidth=5)
        cindex=cindex+1
        hostindex=hostindex+1
        time =[]
        cpupower = []
        mempower = []

lgd = plt.legend(ncol=4,loc="lower center",bbox_to_anchor=(xlegendanchor,ylegendanchor))
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
ylimupper = 240


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
psum=0
powerreadstart=30
powerreadend=85
powerreadend=115
for host in bpower:
    i=0
    for val in host:
        if i > powerreadstart and i < powerreadend :
            psum += val
        i = i + 1

print "total energy consumed " , psum/len(bpower), " Joules"

