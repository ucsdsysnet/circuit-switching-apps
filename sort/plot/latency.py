import numpy as np
from pylab import *
from matplotlib.pyplot import figure
import matplotlib.pyplot as plt
import csv
import sys

def per(p, x, y):
    absval = np.percentile(y, p)
    px = []
    py = []
    for i in xrange(len(y)):
        if y[i] > absval:
            px.append(x[i])
            py.append(y[i])
    return px, py, absval

figure(num=None, figsize=(30, 10), dpi=80, facecolor='w', edgecolor='k')

#ax = plt.gca()
#ax.get_xaxis().get_major_formatter().set_scientific(False)


sys.argv.pop(0)
print str(sys.argv)

plt.rcParams.update({'font.size': 20})

#plot the latencies of each individual measure
color=iter(cm.rainbow(np.linspace(0,1,2*(len(sys.argv) + 1))))
c=next(color)

time = []
data = []
total = []

# calculate 99th percentile
for filename in sys.argv:
    with open(filename,'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')
        i=0
        for row in plots:
            if len(row) >= 2:
                time.append(float(row[0]))
                data.append(float(row[1]))
                total.append(int(row[2]))
        plt.plot(time, data, 'g-', label=filename , color=c)
        plt.plot(time, total, 'g--', label="total GB transfered" , color=c, linewidth=5)
        c=next(color)
        time =[]
        data = []
        total = []
        #plt.plot(dredtime, dredOutstanding, 'g-', label="D-Redundancy client Outstanding" , color='r')
        #plt.plot(dredtime, dredLevel, 'g--', label="D-Redundancy Level" , color='k')



plt.title("TCP Bandwidth, Reading and Writing")
plt.legend()
plt.xlabel("Seconds (S)")
plt.ylabel("Total MegaBit Recevied")
plt.show()



