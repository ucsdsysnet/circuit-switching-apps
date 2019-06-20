import numpy as np
from pylab import *
from matplotlib.pyplot import figure
import matplotlib.pyplot as plt
import csv
import sys
from scipy import stats

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

def GetName(duration, machine, mode, speed):
    return duration + "-" + machine + "-" + mode + "-" +speed

def GetAvName(duration, mode, speed):
    return duration + "-" + mode + "-" +speed

def GetBarefootName(duration, speed):
    return duration + "-barefoot-switch-powermeter-" + speed

def GetRuntimeName(duration, speed):
    return duration + "-" + speed + "-runtime"

def make2dList(rows, cols):
    a=[]
    for row in xrange(rows): a += [[0]*cols]
    return a

def TTest(a, b):
    print(a)
    print(b)
    ## Calculate the Standard Deviation
    #Calculate the variance to get the standard deviation

    #For unbiased max likelihood estimate we have to divide the var by N-1, and therefore the parameter ddof = 1
    var_a = np.var(a)
    var_b = np.var(b)
    print var_a
    print var_b

    #std deviation
    s = np.sqrt((var_a + var_b)/2)



    ## Calculate the t-statistics
    t = (np.mean(a) - np.mean(b))/(s*np.sqrt(2/len(a)))



    ## Compare with the critical t-value
    #Degrees of freedom
    df = 2*len(a) - 2

    #p-value after comparison with the t 
    p = 1 - stats.t.cdf(t,df=df)


    print("t = " + str(t))
    print("p = " + str(2*p))
    return t
    ### You can see that after comparing the t statistic with the critical t value (computed internally) we get a good p value of 0.0005 and thus we reject the null hypothesis and thus it proves that the mean of the two distributions are different and statistically significant.


    ## Cross Checking with the internal scipy function
    t2, p2 = stats.ttest_ind(a,b)
    print("t = " + str(t2))
    print("p = " + str(p2))


#ax = plt.gca()
#ax.get_xaxis().get_major_formatter().set_scientific(False)


sys.argv.pop(0)
#speed=sys.argv.pop(0)
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
ylimupper = 350


tlayx0=0.02
tlayy0=0.24
tlayx1=1
tlayy1=1

chartname="Rapl_CPU_Mem_Power.png"

maparray = [ dict() for x in range(3) ]
avmaparray = [ dict() for x in range(3) ]
fullmaparray = [ dict() for x in range(3) ]
switchmaparray = [ dict() for x in range(3) ]

speeds=["10Gbps","25Gbps","40Gbps"]
measuredurations = ["shuffle", "full"]
measuretypes = ["cpu", "memory", "powermeter"]
machines = ["b09-30","b09-32","b09-34","b09-36"]

shuffletimeoffset=25
sorttimeoffset=26
fulltimeoffset=27

measureindex=0
machineindex=0
measuretypeindex=0
i=0
for x in maparray:
    for md in measuredurations:
        machineindex=0
        for machine in machines:
            measureindex=0
            for mt in measuretypes:
                name = GetName(md,machine,mt,speeds[i])
                if machineindex == 3 and measureindex==2:
                    name = GetBarefootName(md,speeds[i])
                print name
                maparray[i][name] = []
                measureindex = measureindex+1
            machineindex=machineindex+1
        measuretypeindex=measuretypeindex+1
    i=i+1

i=0
for x in maparray:
    for md in measuredurations:
        name=GetRuntimeName(md,speeds[i])
        print(name)
        maparray[i][name] = []
    i= i + 1

# calculate 99th percentile
for filename in sys.argv:
    with open(filename,'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')
        for row in plots:
            if row[0] == "10":
                mapindex = 0
            elif row[0] == "25":
                mapindex = 1
            elif row[0] == "40":
                mapindex = 2


            fullindex=1
            for md in measuredurations:
                machineindex=0
                for machine in machines:
                    measureindex=0
                    for mt in measuretypes:
                        name = GetName(md,machine,mt,speeds[mapindex])
                        if machineindex == 3 and measureindex==2:
                            name = GetBarefootName(md, speeds[mapindex])
                        maparray[mapindex][name].append(float(row[fullindex]))
                        measureindex = measureindex+1
                        fullindex=fullindex+1
                    machineindex=machineindex+1
                measuretypeindex=measuretypeindex+1

            name = GetRuntimeName(measuredurations[0],speeds[mapindex])
            maparray[mapindex][name].append(float(row[shuffletimeoffset]))
            name = GetRuntimeName(measuredurations[1],speeds[mapindex])
            maparray[mapindex][name].append(float(row[fulltimeoffset]))



#print(maparray)


print "measuretype,samples,mean,median,std"

measureindex=0
machineindex=0
measuretypeindex=0
i=0
for x in maparray:
    for md in measuredurations:
        machineindex=0
        for machine in machines:
            measureindex=0
            for mt in measuretypes:
                name = GetName(md,machine,mt,speeds[i])
                if machineindex == 3 and measureindex==2:
                    name = GetBarefootName(md,speeds[i])

                ##CORE
                val = maparray[i][name]
                print ( name + "," +
                        str(len(maparray[i][name])) + "," +
                        str (np.mean(maparray[i][name])) + "," +
                        str(np.median(maparray[i][name])) + "," +
                        str (np.std(maparray[i][name])) )

                ##CORE
                measureindex = measureindex+1
            machineindex=machineindex+1
        measuretypeindex=measuretypeindex+1
    i=i+1

i=0
for x in maparray:
    for md in measuredurations:
        name=GetRuntimeName(md,speeds[i])
        print ( name + "," +
                str(len(maparray[i][name])) + "," +
                str (np.mean(maparray[i][name])) + "," +
                str(np.median(maparray[i][name])) + "," +
                str (np.std(maparray[i][name])) )
    i= i + 1


tenGind = make2dList(4,4)
tfGind = make2dList(4,4)
fourtyGind = make2dList(4,4)

indlist = []
indlist.append(tenGind)
indlist.append(tfGind)
indlist.append(fourtyGind)
## Calculate pairwise independance of each distribution
measureindex=0
machineAindex=0
machineBindex=0
measuretypeindex=0
i=0
for x in maparray:
    for md in measuredurations:
        for mt in measuretypes:
            machineAindex=0
            machineBindex=0
            for machineA in machines:
                machineBindex=0
                for machineB in machines:
                    measureindex=0
                    nameA = GetName(md,machineA,mt,speeds[i])
                    nameB = GetName(md,machineB,mt,speeds[i])
                    if machineAindex == 3 and measureindex==2:
                        nameA = GetBarefootName(md, speeds[i])
                    if machineBindex == 3 and measureindex==2:
                        nameB = GetBarefootName(md, speeds[i])
                    #t = TTest(maparray[i][nameA],maparray[i][nameB])
                    #indlist[i][machineAindex][machineBindex] = t
                    indlist[i][machineAindex][machineBindex] = 0

                    ##CORE
                    ##val = maparray[i][name]
                    ##print ( name + "," +
                    ##        str(len(maparray[i][name])) + "," +
                    ##        str (np.mean(maparray[i][name])) + "," +
                    ##        str(np.median(maparray[i][name])) + "," +
                    ##        str (np.std(maparray[i][name])) )
                    ##
                    ##CORE


                    machineBindex=machineBindex+1
                machineAindex=machineAindex+1
                    
            measureindex = measureindex+1


        measuretypeindex=measuretypeindex+1
    i=i+1

print(indlist)




##Set up average
##Calculate the average power draw of a generic machine
measureindex=0
machineindex=0
measuretypeindex=0
i=0
for x in maparray:
    for md in measuredurations:
        for mt in measuretypes:
            avname = GetAvName(md,mt,speeds[i])
            avmaparray[i][avname] = []
            fullmaparray[i][avname] = 0.0
            switchmaparray[i][avname] = 0.0
            measureindex = measureindex+1
        measuretypeindex=measuretypeindex+1
    i=i+1


##TODO
##Calculate the average power draw of a generic machine
measureindex=0
machineindex=0
measuretypeindex=0
i=0
for x in maparray:
    for md in measuredurations:
        measureindex=0
        for mt in measuretypes:
            machineindex=0
            avname = GetAvName(md,mt,speeds[i])
            #print(avname)
            for machine in machines:
                name = GetName(md,machine,mt,speeds[i])
                #Specially pull out the switch measurement
                if machineindex == 3 and measureindex==2:
                    sname = GetBarefootName(md, speeds[i])
                    print machineAindex, machineBindex
                    print nameA, nameB
                    switchmaparray[i][avname] = np.mean(maparray[i][sname])
                    continue
                if len(avmaparray[i][avname]) == 0:
                    #print machineindex
                    #print measureindex
                    avmaparray[i][avname] = maparray[i][name]
                else:
                    j=0
                    for val in maparray[i][name]:
                        avmaparray[i][avname][j] += val
                        j+=1

                ##CORE
                ##val = maparray[i][name]
                ##print ( name + "," +
                ##        str(len(maparray[i][name])) + "," +
                ##        str (np.mean(maparray[i][name])) + "," +
                ##        str(np.median(maparray[i][name])) + "," +
                ##        str (np.std(maparray[i][name])) )
                ##
                ##print machineAindex, machineBindex
                ##print nameA, nameB
                ##CORE
                machineindex=machineindex+1
            measureindex = measureindex+1
        measuretypeindex=measuretypeindex+1
    i=i+1
print(switchmaparray)
i=0
for s in avmaparray:
    j=0
    for n in s:
        k=0
        for index in s[n]:
            if "powermeter" in n:
                avmaparray[i][n][k] = avmaparray[i][n][k] / 3.0
                avmaparray[i][n][k] = avmaparray[i][n][k] * 8
            else: 
                avmaparray[i][n][k] = avmaparray[i][n][k] / 4.0
                avmaparray[i][n][k] = avmaparray[i][n][k] * 8

            k=k+1
        j=j+1
    i=i+1

i=0
for s in avmaparray:
    j=0
    for n in s:
        if "powermeter" in n:
            ##print switchmaparray[i][n]
            ##print np.mean(avmaparray[i][n])
            aggvalue=str(switchmaparray[i][n] + np.mean(avmaparray[i][n]))
            print n + "," + str (aggvalue)
        j=j+1
    i=i+1

i=0

records=800000000

print("")

i=0
for s in avmaparray:
    j=0
    for n in s:
        if "powermeter" in n:
            ##print switchmaparray[i][n]
            ##print np.mean(avmaparray[i][n])
            aggvalue=str(records / (switchmaparray[i][n] + np.mean(avmaparray[i][n])))
            print n + "," + str (aggvalue)
        j=j+1
    i=i+1
i=0

print("")

for x in maparray:
    for md in measuredurations:
        name=GetRuntimeName(md,speeds[i])
        mean =np.mean(maparray[i][name])/1000000000.0
        std = np.std(maparray[i][name])/1000000000.0
        print ( speeds[i] + "," +
                str (mean) + "," +
                str (std)  )
    i= i + 1



#print avmaparray

#across full and sample calculate an average


##Calculate the total power draw of running sort machine * 9 + Switch
##Calculate the record per jewl of sorting
