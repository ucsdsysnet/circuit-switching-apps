import numpy as np
from pylab import *
from matplotlib.pyplot import figure
import matplotlib.pyplot as plt
import csv
import sys


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
print str(sys.argv)

plt.rcParams.update({'font.size': 30})

#plot the latencies of each individual measure
color=iter(cm.rainbow(np.linspace(0,1,5*(len(sys.argv) + 1))))
c=next(color)


############################################################################
#                       CPU
############################################################################
time = []
data = []
total = []

timeindex=0
cpuindex=1
sendkbytesindex=2
reckbytesindex=3
ramindex=4
diskindex=5

color = ['b','r','c','k', 'm', 'g']
linetype = ['g-','g--','g-+']
cindex=0
lindex=2

#parameters
figx=30
figy=10
dpi=80
figure(num=None, figsize=(figx, figy), dpi=dpi, facecolor='w', edgecolor='k')

xlabel="Seconds"
ylabel="CPU utilization"
title="228Gb Sort 6 Hosts CPU utilization"

xlegendanchor=0.50
ylegendanchor=-0.40

xlimleft = -5
xlimright = 305
ylimlower = -3
ylimupper = 100

chartname="3host_228Gb_cpu.png"

# calculate 99th percentile
for filename in sys.argv:
    with open(filename,'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')

        sname = filename.split("/")
        subname = sname[len(sname)-1]
        leftname = subname.split(".")
        finalname = leftname[0]
        print(finalname)

        i=0
        for row in plots:
            if i == 0:
                i=i+1
                continue
            if len(row) >= 2:
                time.append(get_sec(row[timeindex]))
                data.append(float(row[cpuindex]))
        ntime = normalizeTime(time)
        i=i+1
        plt.plot(ntime, data, linetype[lindex], label=finalname , color=color[cindex], linewidth=2)
        cindex=cindex+1
        time =[]
        data = []
        total = []

lgd = plt.legend(ncol=3,loc="lower center",bbox_to_anchor=(xlegendanchor,ylegendanchor))
plt.grid('on')
plt.tight_layout(rect=(0,-.03,1,1))
plt.title(title)
plt.xlabel(xlabel)
plt.ylabel(ylabel)

plt.xlim(left=xlimleft,right=xlimright)
plt.ylim(bottom=ylimlower,top=ylimupper)
plt.savefig(chartname)
#plt.show()
plt.clf()

#######################################################################################
#                           Network TX Bytes
#######################################################################################
#parameters
figx=30
figy=10
dpi=80
figure(num=None, figsize=(figx, figy), dpi=dpi, facecolor='w', edgecolor='k')

xlabel="Seconds"
ylabel="Network Throughput (Gbps)"
title="228Gb Sort 6 Hosts Network Throughput TX Bytes"

xlegendanchor=0.50
ylegendanchor=-0.40

xlimleft = -5
xlimright = 305
ylimlower = -0.25
ylimupper = 11

cindex=0

chartname="6host_228Gb_TX_Bytes.png"
# calculate 99th percentile
for filename in sys.argv:
    with open(filename,'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')

        sname = filename.split("/")
        subname = sname[len(sname)-1]
        leftname = subname.split(".")
        finalname = leftname[0]

        i=0
        for row in plots:
            if i == 0:
                i=i+1
                continue
            if len(row) >= 2:
                time.append(get_sec(row[timeindex]))
                data.append(float(row[sendkbytesindex]))
        ntime = normalizeTime(time)
        gbps = KbpsToGbps(data)
        i=i+1
        plt.plot(ntime, gbps, linetype[lindex], label=finalname , color=color[cindex], linewidth=3)
        cindex=cindex+1
        time =[]
        data = []
        total = []

lgd = plt.legend(ncol=3,loc="lower center",bbox_to_anchor=(xlegendanchor,ylegendanchor))
plt.grid('on')
plt.tight_layout(rect=(0,-.03,1,1))
plt.title(title)
plt.xlabel(xlabel)
plt.ylabel(ylabel)

plt.xlim(left=xlimleft,right=xlimright)
plt.ylim(bottom=ylimlower,top=ylimupper)
plt.savefig(chartname)

#plt.show()


#######################################################################################
#                           Network RX Bytes
#######################################################################################
figx=30
figy=10
dpi=80
figure(num=None, figsize=(figx, figy), dpi=dpi, facecolor='w', edgecolor='k')

xlabel="Seconds"
ylabel="Network Throughput (Gbps)"
title="228Gb Sort 6 Hosts Network Throughput RX Bytes"

xlegendanchor=0.50
ylegendanchor=-0.40

xlimleft = -5
xlimright = 305
ylimlower = -0.25
ylimupper = 11

cindex=0

chartname="6host_228Gb_RX_Bytes.png"
# calculate 99th percentile
for filename in sys.argv:
    with open(filename,'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')

        sname = filename.split("/")
        subname = sname[len(sname)-1]
        leftname = subname.split(".")
        finalname = leftname[0]

        i=0
        for row in plots:
            if i == 0:
                i=i+1
                continue
            if len(row) >= 2:
                time.append(get_sec(row[timeindex]))
                data.append(float(row[reckbytesindex]))
        ntime = normalizeTime(time)
        gbps = KbpsToGbps(data)
        i=i+1
        plt.plot(ntime, gbps, linetype[lindex], label=finalname , color=color[cindex], linewidth=3)
        cindex=cindex+1
        time =[]
        data = []
        total = []

lgd = plt.legend(ncol=3,loc="lower center",bbox_to_anchor=(xlegendanchor,ylegendanchor))
plt.grid('on')
plt.tight_layout(rect=(0,-.03,1,1))
plt.title(title)
plt.xlabel(xlabel)
plt.ylabel(ylabel)

plt.xlim(left=xlimleft,right=xlimright)
plt.ylim(bottom=ylimlower,top=ylimupper)
plt.savefig(chartname)

#plt.show()
plt.clf()

#######################################################################################
#                           Memory Usage
#######################################################################################
figx=30
figy=10
dpi=80
figure(num=None, figsize=(figx, figy), dpi=dpi, facecolor='w', edgecolor='k')

xlabel="Seconds"
ylabel="Memory Utilization % (126GB/host)"
title="228Gb Sort 6 Hosts Network Memory Usage"

xlegendanchor=0.50
ylegendanchor=-0.40

xlimleft = -5
xlimright = 305
ylimlower = -0.25
ylimupper = 105

cindex=0

chartname="6host_228Gb_Memory_Usage.png"
# calculate 99th percentile
for filename in sys.argv:
    with open(filename,'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')

        sname = filename.split("/")
        subname = sname[len(sname)-1]
        leftname = subname.split(".")
        finalname = leftname[0]

        i=0
        for row in plots:
            if i == 0:
                i=i+1
                continue
            if len(row) >= 2:
                time.append(get_sec(row[timeindex]))
                data.append(float(row[ramindex]))
        ntime = normalizeTime(time)
        i=i+1
        plt.plot(ntime, data, linetype[lindex], label=finalname , color=color[cindex], linewidth=3)
        cindex=cindex+1
        time =[]
        data = []
        total = []

lgd = plt.legend(ncol=3,loc="lower center",bbox_to_anchor=(xlegendanchor,ylegendanchor))
plt.grid('on')
plt.tight_layout(rect=(0.02,-.05,1,1))
plt.title(title)
plt.xlabel(xlabel)
plt.ylabel(ylabel)

plt.xlim(left=xlimleft,right=xlimright)
plt.ylim(bottom=ylimlower,top=ylimupper)
plt.savefig(chartname)

plt.show()
plt.clf()


#######################################################################################
#                           Disk Usage
#######################################################################################
figx=30
figy=10
dpi=80
figure(num=None, figsize=(figx, figy), dpi=dpi, facecolor='w', edgecolor='k')

xlabel="Seconds"
ylabel="Disk Utilization GBps"
title="228Gb Sort 6 Hosts Disk Utilization"

xlegendanchor=0.50
ylegendanchor=-0.40

xlimleft = -5
xlimright = 305
ylimlower = -0.25
ylimupper = 105

cindex=0

chartname="6host_228Gb_Disk_Util.png"
# calculate 99th percentile
for filename in sys.argv:
    with open(filename,'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')

        sname = filename.split("/")
        subname = sname[len(sname)-1]
        leftname = subname.split(".")
        finalname = leftname[0]

        i=0
        for row in plots:
            if i == 0:
                i=i+1
                continue
            if len(row) >= 2:
                time.append(get_sec(row[timeindex]))
                data.append(float(row[diskindex]))
        ntime = normalizeTime(time)
        i=i+1
        plt.plot(ntime, data, linetype[lindex], label=finalname , color=color[cindex], linewidth=3)
        cindex=cindex+1
        time =[]
        data = []
        total = []

lgd = plt.legend(ncol=3,loc="lower center",bbox_to_anchor=(xlegendanchor,ylegendanchor))
plt.grid('on')
plt.tight_layout(rect=(0,-.03,1,1))
plt.title(title)
plt.xlabel(xlabel)
plt.ylabel(ylabel)

plt.xlim(left=xlimleft,right=xlimright)
plt.ylim(bottom=ylimlower,top=ylimupper)
plt.savefig(chartname)

plt.show()
plt.clf()
