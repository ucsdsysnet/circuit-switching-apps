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
	WriteOutputToFile = true
	AsyncWrite        = true
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

//command line usage
var usage = "main.go hostname hosts.txt"

//File to read integers from
var transferFilename = "/home/stew/data/medium-nothing.dat"

//Size of byte buffers for reading and writing TODO this size should be larger for big sort sizes
const BUFSIZE = 4096 * 32

//The total number of integers to generate and sort per node
const INTEGERS = 5000000000
const SORTBUFSIZE = 4096 * 64
const BYTE2INT64CONVERSION = 8
const SORTBUFBYTESIZE = SORTBUFSIZE * BYTE2INT64CONVERSION
const SORTTHREADS = 8
const MAXHOSTS = 15
const BALLENCERATIO = 1.5

//Constants for determining the largest integer value, used to ((aproximatly)) evenly hash integer values across hosts)
const MaxUint = ^uint64(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

const KILLCODE = 123
const KILLMSGSIZE = 1

//SLEEPS
const READWAKETIME = 5
const WRITEWAKETIME = 5
const READTIMEOUT = 10

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

//----------///---------//
type FixedSegment struct {
	buf *[SORTBUFSIZE]uint64
	n   int
}

var filelock sync.Mutex
var toSortLock sync.Mutex

//read buffers one per host
var rbufs [][]byte

//base port all others are offset from this by hostid * hosts + hostid
var basePort = 10940

//All generated integers, some will be sent, some are sorted locally
var toSend = make([]uint64, INTEGERS)

//To sort are the integers which will be sorted locally. Remote values for this
//node are eventually placed into toSort
var toSort = make([]uint64, INTEGERS*BALLENCERATIO)

func main() {
	log.Println("STARTING V2")
	flag.Parse()
	//cpu profileing
	profile.ProfilePath("./cpu.prof")
	defer profile.Start().Stop()

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
	rbufs = make([][]byte, len(ipMap))
	for i := range rbufs {
		//TODO be smart and exclude yourself from this buffer allocation: host i need no buffer for host i
		rbufs[i] = make([]byte, SORTBUFSIZE)
	}

	writeTo := make([]chan FixedSegment, len(ipMap))
	readDone := make(chan Segment, 64)
	writeDone := make([]chan bool, len(ipMap))
	progressReading := make([]chan bool, len(ipMap))
	if ConnectToPeers {
		//Launch TCP listening threads for each other host (one way tcp for ease)
		for remoteHost, remoteIndex := range indexMap {
			//Don't connect with yourself silly!
			if indexMap[hostname] == indexMap[remoteHost] {
				continue
			}
			progressReading[indexMap[remoteHost]] = make(chan bool, 1)
			go ListenRoutine(readDone, progressReading[indexMap[remoteHost]], remoteIndex, hostname, ipMap, indexMap, ports)
		}

		//Sleep so that all conections get established TODO if a single host
		//does not wake up the sort breaks: develop an init protocol for safety
		log.Printf("[%s] Sleeping for %ds to wait for other hosts to wake up", hostname, READWAKETIME)
		time.Sleep(time.Second * READWAKETIME)

		//Alloc an array of outgoing channels to write to other hosts.
		for i := 0; i < len(ipMap); i++ {
			writeTo[i] = make(chan FixedSegment, 1)
			writeDone[i] = make(chan bool, 1)
		}

		thisHostId := indexMap[hostname]
		for remoteHost, remoteAddr := range ipMap {
			//Don't connect with yourself you have your integers!
			if indexMap[hostname] == indexMap[remoteHost] {
				continue
			}
			remoteHostIndex := indexMap[remoteHost]
			remotePort := ports[thisHostId][remoteHostIndex] + basePort
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

	log.Printf("[%s] Sleeping for an additional %ds to wait for other hosts connect their writing channels", hostname, READWAKETIME)
	time.Sleep(time.Second * WRITEWAKETIME)

	totalHosts := len(ipMap)
	log.Println("Total Hosts %d", totalHosts)
	randStart := make(chan bool, totalHosts)
	randStop := make(chan bool, totalHosts)
	// shuffel code

	log.Printf("Generating %s bytes of data for sorting\n", totaldata())
	rand.Seed(int64(time.Now().Nanosecond()))
	dataGenTime := time.Now()
	for i := 0; i < SORTTHREADS; i++ {
		chunksize := (len(toSend) / SORTTHREADS)
		min := i * chunksize
		max := (i + 1) * chunksize
		threadRand := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
		go randomize(toSend[min:max], threadRand, randStart, randStop)
	}
	for i := 0; i < SORTTHREADS; i++ {
		randStart <- true
	}
	for i := 0; i < SORTTHREADS; i++ {
		<-randStop
	}
	dataGenTimeTotal := time.Now().Sub(dataGenTime)
	log.Printf("Done Generating DataSet in %0.3f seconds rate = %0.3fMB/s\n",
		dataGenTimeTotal.Seconds(),
		float64(totaldataVal())/dataGenTimeTotal.Seconds())

	localSortedCounter := make([]int, SORTTHREADS)

	startShuffle := make(chan bool, totalHosts)
	stopShuffle := make(chan bool, totalHosts)
	go func() {
		//Inject a function here which subdivides the input and puts it into buffers for each of the respective receving hosts.
		//Alloc channels for threads to communicate back to the main thread of execution

		for i := 0; i < SORTTHREADS; i++ {
			chunksize := (len(toSend) / SORTTHREADS)
			min := i * chunksize
			max := (i + 1) * chunksize
			go shuffler(toSend[min:max], &(localSortedCounter[i]), totalHosts, i, indexMap[hostname], startShuffle, stopShuffle, writeTo, writeDone)
		}
		//HASHING CODE
		//fmt.Printf("Passing over %s of data to hash to hosts\n", totaldata())
		//time.Sleep(time.Second)
		dataHashTime := time.Now()
		//Determine which integers are going where
		for i := 0; i < SORTTHREADS; i++ {
			startShuffle <- true
		}
		//time.Sleep(time.Second)
		log.Println("All Hashes Started")
		for i := 0; i < SORTTHREADS; i++ {
			<-stopShuffle
		}
		log.Println("All Hashes Ended")

		dataHashTimeTotal := time.Now().Sub(dataHashTime)
		log.Printf("Done Hashing Data across hosts in %0.3f seconds rate = %0.3fMB/s\n",
			dataHashTimeTotal.Seconds(),
			float64(totaldataVal())/dataHashTimeTotal.Seconds())

		//This is where we kill all of the readers.  We are going to use
		//writeTo and write done in a single itteration similar to how we flush
		//the buffer in the last itteration of write. This is done at the aggregate layer so individual thereads dont end connections.

		var endbuf [SORTBUFSIZE]uint64
		var hosts = totalHosts
		endbuf[0] = KILLCODE
		for i := 0; i < hosts; i++ {
			if i == indexMap[hostname] {
				continue
			}
			writeTo[i] <- FixedSegment{buf: &endbuf, n: KILLMSGSIZE}
			//log.Println("KILLING")
		}
		for i := 0; i < hosts-1; i++ {
			<-writeDone[i]
			//log.Println("Finally Moving Forward FINAL!")
		}

		//End Kill Code

	}()
	//\HASHING CODE

	//Wait for transmission of other hosts to end, and count the number of
	//bytes transmitted by othere hosts to perform a single decode operation

	//TODO I'm not sure this is 100 percent safe, in some cases an error could
	//occur, and the end transmission might be triggered (this is a pragmatic
	//approach to get the sort done, just restart if there is a crash.

	//Now each of the reads should be coppied back into a shared buffer, to begin lets use a new toSort
	remoteBufCount := make([]int64, len(ipMap))
	doneHosts := make([]bool, len(ipMap))
	log.Println("Launching a read terminating thread")
	var doneReading chan bool
	doneReading = make(chan bool, 1)
	var totalRead int
	go asyncRead(doneReading, readDone, doneHosts, &toSort, remoteBufCount, &totalRead)

	log.Println("Waiting to finish read before continuing")
	<-doneReading
	log.Println("Reads done")

	//decode via unsafe pointer
	log.Println("Beginning APPEND!")

	for i := range localSortedCounter {
		log.Printf("Number of local values for thread %d =%d\n", i, localSortedCounter[i])
	}

	//This part consolodates locally sorted integers into the original data array
	var trueindex = 0
	for i := 0; i < SORTTHREADS; i++ {
		chunksize := (len(toSend) / SORTTHREADS)
		min := i * chunksize
		for j := 0; j < localSortedCounter[i]; j++ {
			toSend[trueindex] = toSend[min+j]
			trueindex++
		}
	}
	//log.Printf("inplace sorted array %s\n", toSend[0:trueindex])
	//TODO remove appends

	/*
		var bytestamp [SORTBUFBYTESIZE]byte
		var i int
		for i = 0; i < len(toSortBytes); i++ {
			if i%SORTBUFBYTESIZE == 0 && i > 0 {
				intstamp := (*[SORTBUFSIZE]uint64)(unsafe.Pointer(&bytestamp))
				toSort = append(toSort, (*intstamp)[:]...)
			}
			bytestamp[i%SORTBUFBYTESIZE] = toSortBytes[i]
		}
		intstamp := (*[SORTBUFSIZE]uint64)(unsafe.Pointer(&bytestamp))
		toSort = append(toSort, (*intstamp)[:((i/BYTE2INT64CONVERSION)%SORTBUFSIZE)]...)
	*/

	//Save this for last :) TODO
	copy(toSort[totalRead:(totalRead+trueindex)], toSend[0:trueindex])

	//decode toSortBytes
	//Stamping Structs

	//Call the standard library sort and sort everything
	log.Println("Starting Sort!!")
	if ActuallySort {
		sort.Sort(DirRange(toSort))
	}

	if WriteOutputToFile {
		writeOutputToFile(toSort, indexMap[hostname])
	}

	log.Println("Sort Complete!!!")
	return
}

//readDone := make(chan Segment, 64)
func asyncRead(readchan chan bool, readDone chan Segment, doneHosts []bool, toSort *[]uint64, remoteBufCount []int64, sortIndex *int) {
	var total int64
	var lencpy int
	started := false
	readingTime := time.Now()
	var seg Segment
	for {
		if !started && total > 10000 {
			started = true
			readingTime = time.Now()
		}
		seg = <-readDone
		total += seg.n
		//seg.n == -1 means a host is done sending
		if seg.n == -1 {
			doneHosts[seg.index] = true
			if checkdone(doneHosts) {
				break
			}
		} else {

			//copy(toSort[sortIndex:(seg.n/BYTE2INT64CONVERSION)], *(*[]uint64)(unsafe.Pointer(&rbufs[seg.index][:seg.n])))
			lencpy = int(seg.n / BYTE2INT64CONVERSION)
			/*
				for i := sortIndex; i < sortIndex+lencpy+50; i++ {
					log.Printf("index %d - val %d", i, (*toSort)[i])
				}
				log.Printf("Len ToSort = %d, Index %d, size %d", len(*toSort), sortIndex, lencpy)
				log.Println(len(*toSort))
				log.Println((*toSort)[sortIndex:(sortIndex + lencpy)])
			*/
			copy((*toSort)[(*sortIndex):((*sortIndex)+lencpy)], *(*[]uint64)(unsafe.Pointer(&rbufs[seg.index])))
			(*sortIndex) += lencpy

			//toSortBytes = append(toSortBytes, rbufs[seg.index][:seg.n]...)
			//progressReading[seg.index] <- true
			remoteBufCount[seg.index] += seg.n
		}
	}
	readingTimeTotal := time.Now().Sub(readingTime)
	log.Printf("Done Reading data from other hosts in %0.3f seconds rate = %0.3fMB/s, total %dMB\n",
		readingTimeTotal.Seconds(),
		float64(((total*8)/(1024*1024)))/readingTimeTotal.Seconds(),
		(total*8)/(1024*1024))
	readchan <- true

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

func randomize(data []uint64, r *rand.Rand, start, stop chan bool) {
	<-start
	for i := range data {
		data[i] = uint64(r.Int())
		//data[i] = uint64(i) + 8000000000000000000
	}
	stop <- true
}

//func shuffler(data []int, outputBuffers [][]int, hostIndex []int, hosts int, threadIndex int, done chan bool) {
func shuffler(data []uint64, localSortIndexRef *int, hosts int, threadIndex int, myIndex int, start, stop chan bool, writeTo []chan FixedSegment, doneWrite []chan bool) {
	var (
		outputBuffers [MAXHOSTS][SORTBUFSIZE]uint64
		hostIndex     [MAXHOSTS]int
	)
	var sorteeHost int
	var bufIndex int
	<-start
	/*
		outputBuffers := make([][]int, hosts)
		hostIndexs := make([]int, hosts)
		log.Println("Total Hosts %d",totalHosts)
		for i := range outputBuffers {
			outputBuffers[i] = make([]int, SORTBUFSIZE)
			hostIndexs[i] = 0
		}
	*/

	//The next step forward is to keep all local data in the same buffer. To do
	//this data in the input buffer should be overwritten with local data
	//rather than overwriting toSort. The modification goes as thus.

	//1) Each thread write to it's own pre area of the shared data with a variable localSort index

	var localSortIndex = 0

	quant := uint64((MaxInt / hosts))
	for i := 0; i < len(data); i++ {
		//sorteeHost = data[i] / ((MaxInt) / hosts)
		sorteeHost = int(data[i] / quant)

		//Prevent the need to alloc more data for local sorting by overwriting input buffer
		if myIndex == sorteeHost {
			data[localSortIndex] = data[i]
			localSortIndex++
		} else {
			//log.Println("Sortee Host for %d is %d", data[i], sorteeHost)
			bufIndex = hostIndex[sorteeHost]
			outputBuffers[sorteeHost][bufIndex] = data[i]
			hostIndex[sorteeHost]++
			//SlowSend
			if hostIndex[sorteeHost] == SORTBUFSIZE {
				//log.Printf("[%d/100]\n", (int((float32(i) / float32(len(data)) * 100.0))))
				hostIndex[sorteeHost] = 0
				//log.Printf("Writing to %d sending", sorteeHost)
				writeTo[sorteeHost] <- FixedSegment{buf: &outputBuffers[sorteeHost], n: SORTBUFSIZE}
				//log.Printf("Writing waiting to send %d", sorteeHost)
				if !AsyncWrite {
					<-doneWrite[sorteeHost]
				}
			}
		}

	}
	//log.Printf("Returning from Shuffle Thread")
	//log.Printf("Locally stored indexes %d on host %d \n", localSortIndex, myIndex)
	toSortLock.Lock()
	*localSortIndexRef = localSortIndex
	toSortLock.Unlock()
	//Drain remaining buffers
	for i := 0; i < hosts; i++ {
		if i == myIndex {
			continue
		}
		writeTo[i] <- FixedSegment{buf: &outputBuffers[i], n: hostIndex[i]}
		//log.Printf("Writing to %d sending %d Flush", i, hostIndex[i])
		if !AsyncWrite {
			<-doneWrite[i]
		}
		//log.Println("Finally Moving Forward 1")
	}
	//Don't wait for a message to be sent to yourself
	/*
		for i := 0; i < hosts-1; i++ {
			<-doneWrite[i]
			log.Println("Finally Moving Forward 2")
		}*/

	stop <- true
}

//Async write routine. Don't be fooled by the simplicity this is complicated.
//Each routine is pinned to a TCP connection. Upon reciving a segment on an
//async channel this function writes a region of memory referenced by the
//segment to indexed host.
func WriteRoutine2(writeTo chan FixedSegment, doneWriting chan bool, conn net.Conn) {

	for {
		seg := <-writeTo
		//log.Printf("Received Writing Segement of size %d", seg.n)
		buf := (*[SORTBUFBYTESIZE]byte)(unsafe.Pointer(seg.buf))
		//log.Println(buf)
		_, err := conn.Write(buf[:(seg.n * BYTE2INT64CONVERSION)])
		if err != nil {
			//log.Println(err)

			log.Fatal(err)
		}
		//Just Return

		//log.Printf("Returning Control for seg of size %d", seg.n)
		if !AsyncWrite {
			doneWriting <- true
		}
	}
}

func ListenRoutine(readDone chan Segment, progress chan bool, remoteHostIndex int, hostname string, hostIpMap map[string]string, indexMap map[string]int, ports [][]int) {

	var total int64
	var n int
	var err error

	f, err := os.Create(fmt.Sprintf("%s-bw.dat", hostname))
	defer f.Close()

	//This part is special. In the future there should be a big block of mmapped memory for each of the listen routines
	thisHostId := indexMap[hostname]
	//reminder if a -> b than  b listens on ports [a][b]
	port := ports[remoteHostIndex][thisHostId] + basePort
	log.Printf("Starting TCP Connection Listener On %s for remote host index %d on port %d", hostname, remoteHostIndex, port)
	ln, err := net.Listen("tcp4", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listen Complete\n")
	conn, err := ln.Accept()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Connection Accepted\n")
	// TODO bring this back // defer conn.Close()

	//start := time.Now()
	conn.SetReadDeadline(time.Now().Add(10000 * time.Second))
	for {
		//This is going to cause a bug TODO total will overflow rbufs
		n, err = conn.Read(rbufs[remoteHostIndex])
		total += int64(n)
		//log.Printf("%s has received %d integers", hostname, total/8)
		//check For a termination Condition

		//The first termination condition is one where we have called for the execution of the program to end with a kill command

		if n == 1 && rbufs[remoteHostIndex][0] == KILLCODE {
			log.Printf("Received Kill Closing Connection with Host %d\n", remoteHostIndex)
			//break
		}

		if detectTCPClose(conn) {
			//continue
			readDone <- Segment{offset: -1, n: -1, index: remoteHostIndex}
			break
		}
		if err != nil {
			log.Println(err)
			break //TODO the key control break, relies on getting a tcp timeout
		}

		readDone <- Segment{offset: total, n: int64(n), index: remoteHostIndex}
		//<-progress
	}
	readDone <- Segment{offset: total, n: -1, index: remoteHostIndex}
}

func writeOutputToFile(sorted []uint64, hostIndex int) {
	//Write sorted integers out to a file corresponding to the range of sorted
	//integers. These files can be concated for the full sort to be seen.
	f, err := os.Create(fmt.Sprintf("%d.sorted", hostIndex))
	if err != nil {
		log.Fatal("Unable to open output file %s : Error %s")
	}
	for i := range toSort {
		f.WriteString(fmt.Sprintf("%d\n", toSort[i]))
	}
}

//count how many hosts are done
func checkdone(hostsDone []bool) bool {
	count := 0
	for i := range hostsDone {
		if hostsDone[i] {
			count++
		}
	}
	if count >= len(hostsDone)-1 {
		return true
	}
	return false
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
		//log.Printf("%s detected closed LAN connection", "ID")
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
	return (INTEGERS * 8) / (1024 * 1024)
}

type DirRange []uint64

func (a DirRange) Len() int           { return len(a) }
func (a DirRange) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a DirRange) Less(i, j int) bool { return a[i] < a[j] }
