package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/pkg/profile"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var (
	//Configuration parameters
	ActuallySort      = true
	ConnectToPeers    = true
	WriteOutputToFile = false
	AsyncWrite        = true
	SPEEDTEST         = false
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

//command line usage
var usage = "main.go hostname hosts.txt"

//File to read integers from
var transferFilename = "/home/stew/data/medium-nothing.dat"

//Size of byte buffers for reading and writing TODO this size should be larger for big sort sizes
const BUFSIZE = 4096

//The total number of integers to generate and sort per node
//const ITEMS = 550000000
//const ITEMS = 250000000
const ITEMS = 100000000

//const ITEMS = 550000

//const ITEMS = 40000

//const ITEMS = 5000
const SORTBUFSIZE = 4096
const SORTBUFBYTESIZE = SORTBUFSIZE * ITEMSIZE
const SORTTHREADS = 24

const SHUFFLERTHREADS = 18
const RECTHREADS = 4

const RANDTHREADS = 24
const MAXHOSTS = 6
const BALLENCERATIO = 2

const RBUFS = 128
const RBUFCHANS = 12

//Constants for determining the largest integer value, used to ((aproximatly)) evenly hash integer values across hosts)
const MaxUint = ^uint64(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

const MaxUint8 = ^uint8(0)
const MinUint8 = 0
const MaxInt8 = uint(MaxUint8 >> 1)

const KILLMSGSIZE = 1

//SLEEPS
const READWAKETIME = 5
const WRITEWAKETIME = 5
const READTIMEOUT = 20

const KEYSIZE = 10
const VALUESIZE = 400
const ITEMSIZE = KEYSIZE + VALUESIZE

const HOSTS6 = 6

const CONSPERHOST = 3
const PORTOFFSET = 1000

type Item struct {
	Key   [KEYSIZE]byte
	Value [VALUESIZE]byte
}

func (i *Item) Size() int {
	return len(i.Key) + len(i.Value)
}

func (i *Item) String() string {
	return fmt.Sprintf("Key[%X]:Value[%X]", i.Key, i.Value)
}

/* The segement type is used to index into buffers. Reads and Writes from
* buffers are done using segments as a means to determing the offset and
* howmuch to write. The index variable relates to the hosts index buffer which
* will be read or written to. Segments are used (preemtivly) for performance
* purposes so that mulliple reads and writes can be batched into contiguous
* segements. Segemnts are passed along channels so that read / writes can be
* done asyncronsouly without blocking*/
type Segment struct {
	offset int64
	n      int64
	index  int
}

//NOTE Second itteration. There is no need to do anything with offsets, or
//indexes. It does not matter who sent the data, all that matters is the thread
//that it is going to.
type Segment2 struct {
	buf      *[]byte
	bufindex int
	n        int64
}

//----------///---------//
type FixedSegment struct {
	buf *[SORTBUFSIZE]Item
	n   int
}

var filelock sync.Mutex
var recBufLock sync.Mutex

var tLock = make([]sync.Mutex, SORTTHREADS)

//read buffers one per host
//var rbufs [RBUFS][SORTBUFBYTESIZE]byte
var rbufs [][]byte

//base port all others are offset from this by hostid * hosts + hostid
var basePort = 11400

//All generated integers, some will be sent, some are sorted locally
var toSend = make([]Item, ITEMS)

//To sort are the integers which will be sorted locally. Remote values for this
//node are eventually placed into toSort
//var toSort = make([]Item, ITEMS*BALLENCERATIO)
var toSort = make([][]Item, SORTTHREADS)

func main() {
	log.Println("STARTING V2")
	flag.Parse()
	//cpu profileing

	//Remove filename from arguments
	args := os.Args[1:]
	//Parse arguments
	hostname := args[0]
	//Generate a map of hostnames -> ip and hostname -> host index // TODO this is slow index with arrays in the future
	ipMap, indexMap := parseHosts(args[1])

	// 2-D grid of port pairs arranged so the sender is the first index, and the receiver the second
	// [ [0,0] [0,1] ]  // [ 0 1 ]
	// [ [1,0] [1,1] ]  // [ 2 3 ]
	// for an index like [1,0] it reads host1 connects to host0 on that port ->
	// host 0 will be listening on it
	ports := getPortPairs(ipMap)
	//Set up logg7r
	log.SetPrefix("[" + hostname + "] ")

	//Dynamcially allocate buffers TODO refactor to statically allocated
	//buffers, there is no guarentee these are contiguous, and will kill
	//caching performance. In the end these should all be layered over a single
	//piece of mmaped memory and indexed into using an overlay.

	rbufs = make([][]byte, RBUFS)
	for i := range rbufs {
		//TODO be smart and exclude yourself from this buffer allocation: host i need no buffer for host i
		rbufs[i] = make([]byte, SORTBUFBYTESIZE)
	}

	//func rbufpool(request chan bool, requestResponse chan Segment2, ret chan Segment2, retResponse chan bool) {
	rbufRequest := make(chan bool, RBUFCHANS)
	rbufRequestResponse := make(chan Segment2, RBUFCHANS)
	rbufReturn := make(chan Segment2, RBUFCHANS)
	rbufReturnResponse := make(chan bool, RBUFCHANS)
	go rbufpool(rbufRequest, rbufRequestResponse, rbufReturn, rbufReturnResponse)

	//Setup corresponding sort threads
	for i := 0; i < SORTTHREADS; i++ {
		toSort[i] = make([]Item, (ITEMS*BALLENCERATIO)/SORTTHREADS)
	}

	writeTo := make([]chan FixedSegment, len(ipMap))
	readDone := make(chan Segment2, 64)
	writeDone := make([]chan bool, len(ipMap))
	if ConnectToPeers {
		//Launch TCP listening threads for each other host (one way tcp for ease)
		for remoteHost, remoteIndex := range indexMap {
			//Don't connect with yourself silly!
			if indexMap[hostname] == indexMap[remoteHost] {
				continue
			}
			for i := 0; i < CONSPERHOST; i++ {
				go ListenRoutine(readDone, remoteIndex, hostname, ipMap, indexMap, ports, i, rbufRequest, rbufRequestResponse)
			}
		}

		//Alloc an array of outgoing channels to write to other hosts.
		for i := 0; i < len(ipMap); i++ {
			writeTo[i] = make(chan FixedSegment, 1)
			writeDone[i] = make(chan bool, 1)
		}

		//Sleep so that all conections get established TODO if a single host
		//does not wake up the sort breaks: develop an init protocol for safety
		log.Printf("[%s] Sleeping for %ds to wait for other hosts to wake up", hostname, READWAKETIME)
		time.Sleep(time.Second * READWAKETIME)
		thisHostId := indexMap[hostname]

		for remoteHost, remoteAddr := range ipMap {
			//Don't connect with yourself you have your integers!
			if indexMap[hostname] == indexMap[remoteHost] {
				continue
			}

			for i := 0; i < CONSPERHOST; i++ {
				remoteHostIndex := indexMap[remoteHost]
				remotePort := ports[thisHostId][remoteHostIndex] + basePort + (i * PORTOFFSET)
				log.Printf("remote port %d remote addr %s remoteHostIndex %d", remotePort, remoteAddr, remoteHostIndex)
				conn, err := net.Dial("tcp4", fmt.Sprintf("%s:%d", remoteAddr, remotePort))
				if err != nil {
					log.Fatalf("Unable to connect to remote host %s on port %d : Error %s", remoteHost, remotePort, err)
				}
				defer conn.Close()
				log.Printf("Preparing Write Channnel to Host %s, on port %d", remoteHost, remotePort)
				//Launch writing thread

				go WriteRoutine2(writeTo[remoteHostIndex], writeDone[remoteHostIndex], conn)
			}
		}
	}

	log.Printf("[%s] Sleeping for an additional %ds to wait for other hosts connect their writing channels", hostname, READWAKETIME)
	time.Sleep(time.Second * WRITEWAKETIME)

	totalHosts := len(ipMap)
	GenRandomController(totalHosts)

	//Start Listening

	//Start the profile after generating the random data
	defer profile.Start().Stop()
	profile.ProfilePath("./cpu.prof")

	//Wait for transmission of other hosts to end, and count the number of
	//bytes transmitted by othere hosts to perform a single decode operation

	//TODO I'm not sure this is 100 percent safe, in some cases an error could
	//occur, and the end transmission might be triggered (this is a pragmatic
	//approach to get the sort done, just restart if there is a crash.

	//Now each of the reads should be coppied back into a shared buffer, to begin lets use a new toSort
	remoteBufCount := make([]int64, len(ipMap))
	var doneHosts int
	log.Println("Launching a read terminating thread")
	var doneReading chan bool
	var tThreadSortCount = make([][]int, RECTHREADS)
	var threadSortCount = make([]int, SORTTHREADS)
	for i := 0; i < RECTHREADS; i++ {
		tThreadSortCount[i] = make([]int, SORTTHREADS)
	}
	doneReading = make(chan bool, 1)
	var totalRead int
	log.Println("Started Reading")
	for i := 0; i < RECTHREADS; i++ {
		go asyncRead(doneReading, readDone, rbufReturn, rbufReturnResponse, &doneHosts, &toSort, remoteBufCount, &tThreadSortCount[i], &totalRead, len(ipMap), indexMap[hostname])
	}

	//Start Shuffling
	localSortedCounter := make([]int, SORTTHREADS)
	log.Println("Started Shuffle")
	go ShuffleController(totalHosts, &localSortedCounter, hostname, indexMap, writeTo, writeDone, readDone, rbufRequest, rbufRequestResponse)

	//\HASHING CODE

	for i := 0; i < RECTHREADS; i++ {
		<-doneReading
		log.Printf("Rec Thread [%d] Complete", i)
	}

	for i := 0; i < RECTHREADS; i++ {
		for j := 0; j < SORTTHREADS; j++ {
			threadSortCount[j] += tThreadSortCount[i][j]
		}
	}

	//Merge local keys into one continuous chunk
	/*
		var trueindex = 0
		for i := 0; i < SORTTHREADS; i++ {
			chunksize := (len(toSend) / SORTTHREADS)
			min := i * chunksize
			for j := 0; j < localSortedCounter[i]; j++ {
				toSend[trueindex] = toSend[min+j]
				trueindex++
			}
		}*/
	//log.Printf("True Index %d, total Read %d", trueindex, totalRead)
	//Copy locally stored integers into the sorting array
	//copy(toSort[totalRead:(totalRead+trueindex)], toSend[0:trueindex])

	if ActuallySort {
		startSorting := make(chan bool, 1)
		stopSorting := make(chan bool, 1)
		log.Println("Starting Sort!!")
		SortController(&toSort, threadSortCount, startSorting, stopSorting)
		log.Println("Sort Complete!!!")
	}

	if WriteOutputToFile {
		log.Println("Writing to file")
		writeOutputToFile(toSort, indexMap[hostname])
		log.Println("Write Complete")
	}

	return
}

func SortController(toSort *[][]Item, threadRanges []int, sortStart chan bool, sortDone chan bool) {
	for i := 0; i < SORTTHREADS; i++ {
		//go Sorter((*toSort)[i][0:threadRanges[i]], sortStart, sortDone)
		log.Printf("Thread range for %d is %d", i, threadRanges[i])
		go Sorter((*toSort)[i][0:threadRanges[i]], sortStart, sortDone)
	}
	for i := 0; i < SORTTHREADS; i++ {
		sortStart <- true
	}
	for i := 0; i < SORTTHREADS; i++ {
		<-sortDone
	}
	return
}

func Sorter(toSort []Item, sortStart chan bool, sortDone chan bool) {
	<-sortStart
	sort.Sort(DirRange(toSort))
	sortDone <- true
}

func asyncRead(
	readchan chan bool,
	readDone chan Segment2,
	rbufReturn chan Segment2,
	rbufReturnResponse chan bool,
	doneHosts *int,
	toSort *[][]Item,
	remoteBufCount []int64,
	threadIndex *[]int,
	sortIndex *int,
	hosts int,
	hostid int) {

	var total int64
	var lencpy int
	started := false
	readingTime := time.Now()
	var rItems *[]Item
	var seg Segment2

	var tQuant = (uint64((MaxUint / uint64(hosts)))) / SORTTHREADS
	hQuant := uint64((MaxUint / uint64(hosts)))
	var sorteeThread int
	var uintkey uint64

	doneTimeout := time.After(time.Second)
	for {
		if !started && total > 10000 {
			started = true
			readingTime = time.Now()
		}
		select {
		case seg = <-readDone:
			total += seg.n
			//seg.n == -1 means a host is done sending
			if seg.n == -1 {
				recBufLock.Lock()
				(*doneHosts)++
				recBufLock.Unlock()
			} else {
				//Copy from network buffer directly into sorting array
				//NOTE to be sure that this algorithm is working all items of rItems should be checked. The idea is that every set of chunked keys is arriving is SORTBUFSIZE chunks so we only need to look at the first ket to determine which thread it goes to.
				lencpy = int(seg.n / ITEMSIZE)
				/*
					log.Println(unsafe.Pointer(seg.buf))
					log.Println((*seg.buf)[0:10])
					log.Println(*(*[]Item)(unsafe.Pointer(seg.buf)))
					log.Println(*rItems)
				*/
				rItems = (*[]Item)(unsafe.Pointer(seg.buf))

				if seg.buf == nil {
					log.Printf("nil buffer %X", seg.buf)
				}
				//hash out received items to threads
				if seg.n != SORTBUFSIZE*ITEMSIZE {
					log.Fatalf("Received a chunk of size %d, every chunk should be SORTBUFSIZE %d", seg.n, SORTBUFSIZE*ITEMSIZE)
				}

				//NOTE to be sure that this algorithm is working all items of
				//rItems should be checked. The idea is that every set of chunked
				//keys is arriving is SORTBUFSIZE chunks so we only need to look at
				//the first ket to determine which thread it goes to.

				//log.Printf("Received Segment -> %d %d", seg.n, seg.bufindex)
				uintkey = 0
				uintkey += uint64((*rItems)[0].Key[0]) << 56
				uintkey += uint64((*rItems)[0].Key[1]) << 48
				//uintkey += uint64(rItems[0].Key[2]) << 40
				//uintkey += uint64(rItems[0].Key[3]) << 32
				//uintkey += uint64(rItems[0].Key[4]) << 24
				//uintkey += uint64(rItems[0].Key[5]) << 16
				//uintkey += uint64(rItems[0].Key[6]) << 8
				//uintkey += uint64(rItems[0].Key[7]) << 0
				//sorteeThread = int((uintkey / uint64(hostid+1)) / tQuant)
				sorteeThread = int((uintkey - (uint64(hostid) * hQuant)) / (tQuant + 1))
				//BUG
				if sorteeThread > SORTTHREADS-1 || sorteeThread < 0 {
					sorteeThread = 2
				}

				//log.Printf("[%X][%X][%X][%X] sorteeThread %d", rItems[0].Key[0], rItems[0].Key[1], rItems[0].Key[2], rItems[0].Key[3], sorteeThread)

				//log.Printf("Sortee Thread Received %d", sorteeThread)
				//This is where the copy occurs
				//log.Printf("Received Buffer of size %d from %d for thread %d, copying : current size %d len rItems %d", seg.n, seg.index, sorteeThread, (*threadIndex)[sorteeThread], len(rItems))
				//log.Printf("Sortee Thread %d", sorteeThread)
				//log.Printf("Start :%d End: %d", (*threadIndex)[sorteeThread], (*threadIndex)[sorteeThread]+lencpy)

				//copy((*toSort)[sorteeThread][(*threadIndex)[sorteeThread]:((*threadIndex)[sorteeThread]+lencpy)], rItems[:])

				tLock[sorteeThread].Lock()
				copy((*toSort)[sorteeThread][(*threadIndex)[sorteeThread]:((*threadIndex)[sorteeThread]+lencpy)], *(*[]Item)(unsafe.Pointer(seg.buf)))
				//log.Printf("DoneCopy")
				(*threadIndex)[sorteeThread] += lencpy
				(*sortIndex) += lencpy
				//TODO move to listening thread
				//remoteBufCount[seg.index] += seg.n
				tLock[sorteeThread].Unlock()

				rbufReturn <- seg
				<-rbufReturnResponse

			}

		case <-doneTimeout:
			if (*doneHosts) >= ((hosts - 1) * CONSPERHOST) {
				log.Printf("Exiting reading thread")
				readingTimeTotal := time.Now().Sub(readingTime)
				log.Printf("Done Reading data from other hosts in %0.3f seconds rate = %0.3fMB/s, total %dMB\n",
					readingTimeTotal.Seconds(),
					float64(((total*8)/(1024*1024)))/readingTimeTotal.Seconds(),
					(total*8)/(1024*1024))
				readchan <- true
				return
			}
			doneTimeout = time.After(time.Second)
		}
	}

}

//Hash function for determining which integers will be sorted by which hosts.
//This one evenly spaces hosts across the space of integers. TODO to get an
//even spread use a second layer of virtual nodes around the ring. (take a look
//at the dynamo paper
//[https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf])
func getDestinationHost(value uint64, hosts uint64) uint64 {
	dHost := value / ((MaxUint) / hosts)
	return dHost
}

func randomize(data []Item, r *rand.Rand, start, stop chan bool) {
	var datalen = len(data)
	<-start
	for i := 0; i < datalen; i++ {
		r.Read(data[i].Key[:])
		r.Read(data[i].Value[:])
	}
	stop <- true
}

func GenRandomController(totalHosts int) {
	log.Println("Total Hosts %d", totalHosts)
	randStart := make(chan bool, totalHosts)
	randStop := make(chan bool, totalHosts)
	// shuffel code

	log.Printf("Generating %s bytes of data for sorting\n", totaldata())
	rand.Seed(int64(time.Now().Nanosecond()))
	dataGenTime := time.Now()
	for i := 0; i < RANDTHREADS; i++ {
		chunksize := (len(toSend) / RANDTHREADS)
		min := i * chunksize
		max := (i + 1) * chunksize
		threadRand := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
		go randomize(toSend[min:max], threadRand, randStart, randStop)
	}
	for i := 0; i < RANDTHREADS; i++ {
		randStart <- true
	}
	for i := 0; i < RANDTHREADS; i++ {
		<-randStop
	}
	dataGenTimeTotal := time.Now().Sub(dataGenTime)
	log.Printf("Done Generating DataSet in %0.3f seconds rate = %0.3fMB/s\n",
		dataGenTimeTotal.Seconds(),
		float64(totaldataVal())/dataGenTimeTotal.Seconds())
}

/* The shuffler method distributes key across hosts. values from the data array
* are spread evenly across hosts. Values destened for remote hosts are placed
* into buffers, when the buffers are full they are written to the related host.
* Values which are to be sorted locally are written back into the data array at
* the beginning

data - The input data being sorted
localSortIndexRef - the number of values which are stored locally, this value is returned so that local values can be extracted
hosts - the total number of host in the system
threadIndex- shufflers are usually goroutines, this is the id of the goroutine
myIndex - global host index, used to determine which values are local
start - channel used to signal the start of a computation
stop - channel used to signal the stop of a computation
writeTo - Segment channel used to communicate to network writing threads
doneWrite - Used to signal that a write is complete (may compromise safty when async is turned on

*/
func shuffler(data []Item,
	localSortIndexRef *int,
	hosts int,
	threadIndex int,
	myIndex int,
	start,
	stop chan bool,
	writeTo []chan FixedSegment,
	doneWrite []chan bool,
	readDone chan Segment2,

	rbufRequest chan bool,
	rbufRequestResponse chan Segment2,

) {

	var (
		outputBuffers [MAXHOSTS][SORTTHREADS][SORTBUFSIZE]Item
		hostIndex     [MAXHOSTS][SORTTHREADS]int
	)
	var sorteeHost int
	var sorteeThread int
	var bufIndex int

	//TODO kill
	var ringallocation [HOSTS6][SORTTHREADS]int
	<-start

	//The next step forward is to keep all local data in the same buffer. To do
	//this data in the input buffer should be overwritten with local data
	//rather than overwriting toSort. The modification goes as thus.

	//1) Each thread write to it's own pre area of the shared data with a variable localSort index

	var datalen = len(data)
	var uintkey uint64

	hQuant := uint64((MaxUint / uint64(hosts)))
	tQuant := (uint64((MaxUint / uint64(hosts)))) / SORTTHREADS
	hostDiv := (uint64(hosts) * tQuant)

	var seg Segment2
	for i := 0; i < datalen; i++ {

		//TODO if this is not a bottle neck make it bigger
		uintkey = 0
		uintkey |= uint64(data[i].Key[0]) << 56
		uintkey |= uint64(data[i].Key[1]) << 48
		//uintkey |= uint64(data[i].Key[2]) << 40
		//uintkey |= uint64(data[i].Key[3]) << 32
		//uintkey |= uint64(data[i].Key[4]) << 24
		//uintkey |= uint64(data[i].Key[5]) << 16
		//uintkey |= uint64(data[i].Key[6]) << 8
		//uintkey |= uint64(data[i].Key[7]) << 0

		sorteeHost = int(uintkey / hQuant)
		sorteeThread = int((uintkey / hostDiv))

		ringallocation[sorteeHost][sorteeThread]++

		//log.Printf("uint key %d", uintkey)
		/*
			if i%10000 == 0 {
				log.Printf("%d/%d complete shuffle", i, len(data))
			}*/
		//log.Printf("Hosts %d", hosts)
		//log.Printf("[%X][%X][%X][%X] -- sorteeHost %d, sorteeThread %d", data[i].Key[0], data[i].Key[1], data[i].Key[2], data[i].Key[3], sorteeHost, sorteeThread)

		//log.Printf("Sortee Host %d", sorteeHost)

		//if local write back to the beginning of the data array to save memory

		bufIndex = hostIndex[sorteeHost][sorteeThread]
		outputBuffers[sorteeHost][sorteeThread][bufIndex] = data[i]
		hostIndex[sorteeHost][sorteeThread]++
		//Buffer full time to send
		if hostIndex[sorteeHost][sorteeThread] == SORTBUFSIZE {

			hostIndex[sorteeHost][sorteeThread] = 0
			if myIndex == sorteeHost {
				//TODO acquire a buffer fro the buffer pool
				seg.n = -1
				for seg.n == -1 {
					rbufRequest <- true
					seg = <-rbufRequestResponse
				}
				//have rbuf
				copy((*seg.buf)[:], (*[SORTBUFBYTESIZE]byte)(unsafe.Pointer(&outputBuffers[myIndex][sorteeThread]))[:SORTBUFBYTESIZE])
				readDone <- seg

			} else {
				//log.Printf("Writing to Host[%d][%d]", sorteeHost, sorteeThread)
				//TODO I'm getting an index out of range error right here, this would be a good place to start tomorrow.
				//Write Remote
				//NOTE if there is an index out of range here it is likely because there is not enough static room in the sorteeThread
				//log.Printf("Sending remotely to Host %d thread %d from thread %d", sorteeHost, sorteeThread, threadIndex)
				writeTo[sorteeHost] <- FixedSegment{buf: &outputBuffers[sorteeHost][sorteeThread], n: SORTBUFSIZE}
				//Don't wait for the write to complete if in asyc mode
				//TODO may be unsafe. It's proably better to have some sort of global buffer pool
				if !AsyncWrite {
					<-doneWrite[sorteeHost]
				}
			}
		}
	}

	//log.Println("DRAINING BUFFERS")
	//Drain remaining buffers
	for i := 0; i < hosts; i++ {
		if i == myIndex {
			for j := 0; j < SORTTHREADS; j++ {
				if hostIndex[i][j] == 0 {
					continue
				}
				seg.n = -1
				for seg.n == -1 {
					rbufRequest <- true
					seg = <-rbufRequestResponse
				}
				copy((*seg.buf)[:], (*[SORTBUFBYTESIZE]byte)(unsafe.Pointer(&outputBuffers[myIndex][sorteeThread]))[:SORTBUFBYTESIZE])
				readDone <- seg
			}
		} else {
			for j := 0; j < SORTTHREADS; j++ {
				//log.Printf("Host[%d][%d] = %d", i, j, hostIndex[i][j])
				//writeTo[i] <- FixedSegment{buf: &outputBuffers[i][j], n: hostIndex[i][j]}

				if hostIndex[i][j] == 0 {
					//Don't bother writing to the shared channel
					continue
				}
				//NOTE To write the correct ammount of data here use
				//hostIndex[i][j]. The reasona that I'm using SORTBUFSIZE is so
				//that on the other side all of the received traffic from this
				//node will be the same size. This means that when
				//demultiplexing I can count on the fact that ordered chunks of
				//integers are all allinged in chunks. The downside is that we
				//will aslos be sending some junk. To fix that lets zero the
				//back of the array before sending.

				/*
					for k := hostIndex[i][j] + 1; k < SORTBUFSIZE; k++ {
						log.Println("Zeroing")
						outputBuffers[i][j][k] = Item{} //Zero Item
					}
				*/

				writeTo[i] <- FixedSegment{buf: &outputBuffers[i][j], n: SORTBUFSIZE}
				if !AsyncWrite {
					<-doneWrite[i]
				}
			}
		}

	}
	/*
		for i := 0; i < HOSTS6; i++ {
			log.Printf("Host %d", i)
			for j := 0; j < SORTTHREADS; j++ {
				log.Printf("%d - > %d", j, ringallocation[i][j])
			}
		}
	*/
	//log.Println("Finished Shuffle")
	stop <- true
}

func ShuffleController(totalHosts int, localSortedCounter *[]int, hostname string, indexMap map[string]int, writeTo []chan FixedSegment, writeDone []chan bool, readDone chan Segment2, rbufRequest chan bool, rbufRequestResponse chan Segment2) {
	startShuffle := make(chan bool, totalHosts)
	stopShuffle := make(chan bool, totalHosts)
	//Inject a function here which subdivides the input and puts it into buffers for each of the respective receving hosts.
	//Alloc channels for threads to communicate back to the main thread of execution
	if SPEEDTEST {
		time.Sleep(time.Second * 300)
		return
	}

	for i := 0; i < SHUFFLERTHREADS; i++ {
		chunksize := (len(toSend) / SHUFFLERTHREADS)
		min := i * chunksize
		max := (i + 1) * chunksize
		go shuffler(toSend[min:max], &((*localSortedCounter)[i]), totalHosts, i, indexMap[hostname], startShuffle, stopShuffle, writeTo, writeDone, readDone, rbufRequest, rbufRequestResponse)
	}
	//HASHING CODE
	//fmt.Printf("Passing over %s of data to hash to hosts\n", totaldata())
	//time.Sleep(time.Second)
	dataHashTime := time.Now()
	//Determine which integers are going where
	for i := 0; i < SHUFFLERTHREADS; i++ {
		startShuffle <- true
	}
	//time.Sleep(time.Second)
	log.Println("All Hashes Started")
	for i := 0; i < SHUFFLERTHREADS; i++ {
		<-stopShuffle
	}
	log.Println("All Hashes Ended")

	//Get the garbage collector get grab toSend
	toSend = nil

	dataHashTimeTotal := time.Now().Sub(dataHashTime)
	log.Printf("Done Hashing Data across hosts in %0.3f seconds rate = %0.3fMB/s\n",
		dataHashTimeTotal.Seconds(),
		float64(totaldataVal())/dataHashTimeTotal.Seconds())

}

//Async write routine. Don't be fooled by the simplicity this is complicated.
//Each routine is pinned to a TCP connection. Upon reciving a segment on an
//async channel this function writes a region of memory referenced by the
//segment to indexed host.
func WriteRoutine2(writeTo chan FixedSegment, doneWriting chan bool, conn net.Conn) {
	var seg FixedSegment
	var speedbuf [SORTBUFSIZE]Item
	var err error
	for {
		if SPEEDTEST {
			seg = FixedSegment{buf: &speedbuf, n: SORTBUFSIZE}
		} else {
			seg = <-writeTo
		}

		//seg.n should always = SORYBUFSIZE
		_, err = conn.Write((*[SORTBUFBYTESIZE]byte)(unsafe.Pointer(seg.buf))[:seg.n*ITEMSIZE])
		if err != nil {
			log.Fatal(err)
		}
		if !AsyncWrite {
			doneWriting <- true
		}
	}
}

/*Listen Routine performs all network reading between this host and a single
other host over a TCP connection. This function starts a connection, waits
for a connection to start and then begins reading. Data read from the network
is sent via RBUFs back to the main application.

readDone - Channel of Segments, sends data  back to main application, and signals when the computation is over
remoteHostIndex - The remote host being read from
hostname - this hosts hostname
hostIpMap - maps hosts to IP used for connecting and debugging
indexMap - relates hosts to their global index in the configuration file
ports - port matrix, used to determine which port to listen on.
hostConnection - the nth connection that is being made to the particular host bounded by CONSPERHOST
rbufRequest - channel for making requests to the buffer pool
rbufRequestResponse - channel for sending responses to the async readers
*/
func ListenRoutine(readDone chan Segment2,
	remoteHostIndex int,
	hostname string,
	hostIpMap map[string]string,
	indexMap map[string]int,
	ports [][]int,
	hostConnection int,
	rbufRequest chan bool,
	rbufRequestResponse chan Segment2) {

	var total int64
	var n int
	var err error

	//TODO for clenliness put this into its own function
	//This part is special. In the future there should be a big block of mmapped memory for each of the listen routines
	thisHostId := indexMap[hostname]
	//reminder if a -> b than  b listens on ports [a][b]
	port := ports[remoteHostIndex][thisHostId] + basePort + (PORTOFFSET * hostConnection)
	log.Printf("Starting TCP Connection Listener On %s for remote host index %d on port %d", hostname, remoteHostIndex, port)
	ln, err := net.Listen("tcp4", fmt.Sprintf("%s:%d", hostIpMap[hostname], port))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listen Complete\n")
	conn, err := ln.Accept()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Connection Accepted\n")
	defer conn.Close()

	//start := time.Now()
	conn.SetReadDeadline(time.Now().Add(10000 * time.Second))
	var sortedChunkIndex = 0

	var needBuf = true
	var seg Segment2

	var speedbuf = make([]byte, SORTBUFBYTESIZE)
	for {

		//n, err = conn.Read(rbufs[remoteHostIndex])

		//NOTE originally we would receive with the line about becasue we did
		//not care about the size of the receive. Now that we are multiplexing
		//based on threads however we want to receive exactly SORTBUFSIZE
		//before sending the buffer to the receiving thread.

		//TODO obtain a buffer from the buffer pool
		if SPEEDTEST {
			seg = Segment2{buf: &speedbuf, n: SORTBUFBYTESIZE}
		} else {
			if needBuf {
				rbufRequest <- true
				seg = <-rbufRequestResponse
				if seg.bufindex == -1 {
					//Try again if needBuf and none are available
					continue
				} else {
					needBuf = false
				}
			}
		}

		n, err = conn.Read((*seg.buf)[sortedChunkIndex:(SORTBUFBYTESIZE)])

		if SPEEDTEST {
			continue
		}
		//log.Printf("Received buf of size %d", n)
		total += int64(n)

		if detectTCPClose(conn) {
			seg.n = -1
			readDone <- seg
			//give buf away
			break
		}

		if err != nil {
			log.Println(err)
			break //TODO the key control break, relies on getting a tcp timeout
		}

		//Here we contract the ammount that we want to send on the next itteration if we did not receive enough
		sortedChunkIndex += n
		if sortedChunkIndex != (SORTBUFSIZE * ITEMSIZE) {
			//log.Printf("Sortedchunk != SORTBUFSIZE -> %d != %d", sortedChunkIndex, SORTBUFSIZE*ITEMSIZE)
			//We still want to receive more so contine and receive again with a smaller buffer
			continue
		}
		//Now we have enough that we know the chunk is sorted and ready for
		//a given thread so reset for the next sorted chunk and pass the
		//buffer on.

		//This could just as easily be n: SORTBUFSIZE but i'm using sortedChunk in case there are bugs

		seg.n = int64(sortedChunkIndex)
		readDone <- seg
		sortedChunkIndex = 0
		needBuf = true

	}
	seg.n = -1
	readDone <- seg
}

func writeOutputToFile(sorted [][]Item, hostIndex int) {
	doneWrite := make(chan bool, 1)
	for i := range sorted {
		go WriteOutputToSingleFile(sorted[i], hostIndex, i, doneWrite)
	}
	for range sorted {
		<-doneWrite
	}
}

func WriteOutputToSingleFile(sorted []Item, hostIndex, threadIndex int, doneWrite chan bool) {
	f, err := os.Create(fmt.Sprintf("%d_%d.sorted", hostIndex, threadIndex))
	if err != nil {
		log.Fatal("Unable to open output file %s : Error %s")
	}
	for i := range sorted {
		f.WriteString(sorted[i].String() + "\n")
	}
	doneWrite <- true
}

//Returns a map of hostnames to Ip address, and hostname to host index based on
//a config file
func parseHosts(configFile string) (map[string]string, map[string]int) {
	hostMap := make(map[string]string, 0)
	idMap := make(map[string]int, 0)
	f, err := os.Open(configFile)
	if err != nil {
		log.Fatalf("Unable to read in config file Error:%s\n", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	total := 0
	for scanner.Scan() {
		total++
		line := scanner.Text()
		hostArgs := strings.Split(line, " ")
		hostname := hostArgs[0]
		hostIp := hostArgs[1]
		hostId, err := strconv.Atoi(hostArgs[2])
		if err != nil {
			log.Fatal("Unable to parse host identifier %s to an integer: Error %s", hostArgs[2], err)
		}
		hostMap[hostname] = hostIp
		idMap[hostname] = hostId
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return hostMap, idMap
}

//This function populates the 2d array of port pairs based on host names
func getPortPairs(hosts map[string]string) [][]int {
	ports := make([][]int, len(hosts))
	total := 0
	for i := range ports {
		ports[i] = make([]int, len(hosts))
		for j := range ports[i] {
			ports[i][j] = total
			total++
		}
	}
	fmt.Println(ports)
	return ports
}

//read a file in and return a byte array TODO this may need to be optimized for fast reading of large files
func readInFile(filename string) []byte {
	f, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("Unable to read in file %s ,Error : %s", filename, err)
	}
	return f
}

//This function returns true if the tcp connection has been closed, false otherwise
func detectTCPClose(c net.Conn) bool {
	one := []byte{}
	_, err := c.Read(one)
	if err == nil {
		c.SetReadDeadline(time.Now().Add(time.Second * READTIMEOUT))
		return false
	} else if err == io.EOF {
		log.Printf("%s detected closed LAN connection", "ID")
		c.Close()
		c = nil
		return true
	} else {
		//log.Println(err.Error())
		c.SetReadDeadline(time.Now().Add(time.Second * READTIMEOUT))
		return false
	}
}

//This is a convience function for printing the amount of data being sorted on a single host
func totaldata() string {
	return fmt.Sprintf("%dMB", totaldataVal())
}

//RETURN THE TOTAL AMMOUNT OF DATA IN MB
func totaldataVal() int {
	return (ITEMS * ITEMSIZE * 8) / (1024 * 1024)
}

type DirRange []Item

func (a DirRange) Len() int { return len(a) }
func (a DirRange) Swap(i, j int) {
	for k := range a[i].Key {
		a[i].Key[k], a[j].Key[k] = a[j].Key[k], a[i].Key[k]
	}
	for k := range a[i].Value {
		a[i].Value[k], a[j].Value[k] = a[j].Value[k], a[i].Value[k]
	}
	//a[i], a[j] = a[j], a[i]
}
func (a DirRange) Less(i, j int) bool {
	for k := range a[i].Key {
		if uint8(a[i].Key[k]) < uint8(a[j].Key[k]) {
			//log.Printf("%d < %d", a[i].Key[k], a[j].Key[k])
			return true
		} else if uint8(a[i].Key[k]) > uint8(a[j].Key[k]) {
			return false
		}
	}
	return false
}

//BufferPoolImplementation
var poolResources [RBUFS]bool

func rbufpool(request chan bool, requestResponse chan Segment2, ret chan Segment2, retResponse chan bool) {
	for i := range poolResources {
		poolResources[i] = true
	}
	for {
		select {
		case <-request:
			index := get()
			if index == -1 {
				requestResponse <- Segment2{buf: nil, bufindex: -1, n: -1}
			} else {
				requestResponse <- Segment2{buf: &rbufs[index], bufindex: index, n: SORTBUFBYTESIZE}
			}
		case seg := <-ret:
			insert(seg.bufindex)
			retResponse <- true
		}
	}

}

func insert(index int) {
	poolResources[index] = true
}

func get() int {

	//TODO perhaps walk forward on gets like a ring buffer so that the same
	//memory is not being hammered back and forth.

	for i := range poolResources {
		if poolResources[i] == true {
			poolResources[i] = false
			return i
		}
	}
	return -1
}
