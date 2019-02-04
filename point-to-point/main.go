package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	//"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var (
	//Configuration parameters
	WriteOutputToFile = false
	ConnectToPeers    = true
)

//command line usage
var usage = "main.go hostname hosts.txt"

//File to read integers from
var transferFilename = "/home/stew/data/medium-nothing.dat"

//Size of byte buffers for reading and writing TODO this size should be larger for big sort sizes
const BUFSIZE = 4096

//The total number of integers to generate and sort per node
const INTEGERS = 1000000000
const SIZEOFINT = 4
const SORTBUFSIZE = 4096
const SORTBUFBYTESIZE = SORTBUFSIZE * 4
const SORTTHREADS = 4

//Constants for determining the largest integer value, used to ((aproximatly)) evenly hash integer values across hosts)
const MaxUint = ^uint64(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

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
	buf *[SORTBUFSIZE]int
	n   int
}

var filelock sync.Mutex

//read buffers one per host
var rbufs [][]byte

//base port all others are offset from this by hostid * hosts + hostid
var basePort = 10650

//All generated integers, some will be sent, some are sorted locally
var toSend = make([]int, INTEGERS)

//To sort are the integers which will be sorted locally. Remote values for this node are eventually placed into toSort
var toSort = make([]int, 0)

func main() {
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
		rbufs[i] = make([]byte, BUFSIZE)
	}

	writeTo := make([]chan FixedSegment, len(ipMap))
	readDone := make(chan Segment, (len(ipMap) - 1))
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

		//Sleep so that all conections get established TODO if a single host does
		//not wake up the sort breaks: develop an init protocol for safety
		log.Printf("[%s] Sleeping for a moment to wait for other hosts to wake up", hostname)
		time.Sleep(time.Second * 5)

		//Alloc an array of outgoing channels to write to other hosts.
		for i := 0; i < len(ipMap); i++ {
			writeTo[i] = make(chan FixedSegment, 0)
			writeDone[i] = make(chan bool, 0)
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
			log.Printf("Preparing Write Channnel to Host %s, on port %d", remoteHost, remotePort)
			//Launch writing thread
			go WriteRoutine2(writeTo[remoteHostIndex], writeDone[remoteHostIndex], conn)
		}
	}

	//generate random integers for sorting
	log.Printf("Generating %s bytes of data for sorting\n", totaldata())
	rand.Seed(int64(time.Now().Nanosecond()))
	dataGenTime := time.Now()
	for i := 0; i < INTEGERS; i++ {
		rvar := rand.Int()
		//rvar := i
		toSend[i] = rvar
	}
	dataGenTimeTotal := time.Now().Sub(dataGenTime)
	fmt.Printf("Done Generating DataSet in %0.3f seconds rate = %0.3fMB/s\n",
		dataGenTimeTotal.Seconds(),
		float64(totaldataVal())/dataGenTimeTotal.Seconds())

	go func() {
		//Inject a function here which subdivides the input and puts it into buffers for each of the respective receving hosts.
		//Alloc channels for threads to communicate back to the main thread of execution
		totalHosts := len(ipMap)
		done := make(chan bool, totalHosts)

		for i := 0; i < SORTTHREADS; i++ {
			chunksize := (len(toSend) / SORTTHREADS)
			min := i * chunksize
			max := (i + 1) * chunksize
			go shuffler(toSend[min:max], totalHosts, i, indexMap[hostname], done, writeTo, writeDone)
		}
		//HASHING CODE
		//fmt.Printf("Passing over %s of data to hash to hosts\n", totaldata())
		time.Sleep(time.Second)
		dataHashTime := time.Now()
		//Determine which integers are going where
		for i := 0; i < SORTTHREADS; i++ {
			done <- true
		}
		log.Println("All Hashes Started")
		for i := 0; i < SORTTHREADS; i++ {
			<-done
		}

		dataHashTimeTotal := time.Now().Sub(dataHashTime)
		log.Printf("Done Hashing Data across hosts in %0.3f seconds rate = %0.3fMB/s\n",
			dataHashTimeTotal.Seconds(),
			float64(totaldataVal())/dataHashTimeTotal.Seconds())
	}()
	//\HASHING CODE

	//Wait for transmission of other hosts to end, and count the number of
	//bytes transmitted by othere hosts to perform a single decode operation

	//TODO I'm not sure this is 100 percent safe, in some cases an error could
	//occur, and the end transmission might be triggered (this is a pragmatic
	//approach to get the sort done, just restart if there is a crash.

	//Now each of the reads should be coppied back into a shared buffer, to begin lets use a new toSort
	toSortBytes := make([]byte, 0)
	remoteBufCount := make([]int64, len(ipMap))
	doneHosts := make([]bool, len(ipMap))

	var total int64
	started := false
	readingTime := time.Now()
	var seg Segment
	for {
		if !started {
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
			//log.Printf("Seg Index %d n %d", seg.index, seg.n)
			//toSortBytes = append(toSortBytes, rbufs[seg.index][:seg.n]...)
			progressReading[seg.index] <- true
			remoteBufCount[seg.index] += seg.n
		}
	}
	readingTimeTotal := time.Now().Sub(readingTime)
	log.Printf("Done Reading data from other hosts in %0.3f seconds rate = %0.3fMB/s, total %dMB\n",
		readingTimeTotal.Seconds(),
		float64(((total*8)/(1000*1000)))/readingTimeTotal.Seconds(),
		(total*8)/(1000*1000))

	//decode via unsafe pointer
	var bytestamp [SORTBUFBYTESIZE]byte
	var i int
	for i = 0; i < len(toSortBytes); i++ {
		if i%SORTBUFBYTESIZE == 0 && i > 0 {
			intstamp := (*[SORTBUFSIZE]int)(unsafe.Pointer(&bytestamp))
			toSort = append(toSort, (*intstamp)[:]...)
		}
		bytestamp[i%SORTBUFBYTESIZE] = toSortBytes[i]
	}
	intstamp := (*[SORTBUFSIZE]int)(unsafe.Pointer(&bytestamp))
	toSort = append(toSort, (*intstamp)[:(i%SORTBUFSIZE)]...)

	//decode toSortBytes
	//Stamping Structs

	//Call the standard library sort and sort everything
	log.Println("Starting Sort!!")
	//sort.Ints(toSort)

	if WriteOutputToFile {
		writeOutputToFile(toSort, indexMap[hostname])
	}

	log.Println("Sort Complete!!!")
	return
}

const MAXHOSTS = 15

//Hash function for determining which integers will be sorted by which hosts.
//This one evenly spaces hosts across the space of integers. TODO to get an
//even spread use a second layer of virtual nodes around the ring. (take a look
//at the dynamo paper
//[https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf])
func getDestinationHost(value int, hosts int) int {
	dHost := value / ((MaxInt) / hosts)
	return dHost
}

//func shuffler(data []int, outputBuffers [][]int, hostIndex []int, hosts int, threadIndex int, done chan bool) {
func shuffler(data []int, hosts int, threadIndex int, myIndex int, done chan bool, writeTo []chan FixedSegment, doneWrite []chan bool) {
	var (
		outputBuffers [MAXHOSTS][SORTBUFSIZE]int
		hostIndex     [MAXHOSTS]int
	)
	var sorteeHost int
	var bufIndex int
	<-done
	/*
		outputBuffers := make([][]int, hosts)
		hostIndexs := make([]int, hosts)
		for i := range outputBuffers {
			outputBuffers[i] = make([]int, SORTBUFSIZE)
			hostIndexs[i] = 0
		}
	*/
	quant := (MaxInt / hosts)
	for i := 0; i < len(data); i++ {
		//sorteeHost = data[i] / ((MaxInt) / hosts)
		sorteeHost = data[i] / quant
		//fmt.Printf("Sortee Host %d data[%d] = %d\n", sorteeHost, i, data[i])
		bufIndex = hostIndex[sorteeHost]
		outputBuffers[sorteeHost][bufIndex] = data[i]
		hostIndex[sorteeHost]++

		//SlowSend
		if hostIndex[sorteeHost] == SORTBUFSIZE {
			//log.Printf("[%d/100]\n", (int((float32(i) / float32(len(data)) * 100.0))))
			hostIndex[sorteeHost] = 0
			if myIndex == sorteeHost {
				toSort = append(toSort, outputBuffers[sorteeHost][:SORTBUFSIZE]...)
				continue
			}
			//log.Printf("Writing to %d sending", sorteeHost)
			writeTo[sorteeHost] <- FixedSegment{buf: &outputBuffers[sorteeHost], n: SORTBUFSIZE}
			//log.Printf("Writing waiting to send %d", sorteeHost)
			<-doneWrite[sorteeHost]
			//log.Printf("Moving Forward %d ", sorteeHost)
		}

	}
	for i := 0; i < hosts; i++ {
		//log.Printf("hostIndex[%d]:%d -  ", i, hostIndex[i])
		if myIndex == i {
			toSort = append(toSort, outputBuffers[i][:hostIndex[i]]...)
			continue
		}
		//log.Println("Final Write")
		writeTo[i] <- FixedSegment{buf: &outputBuffers[i], n: hostIndex[i]}
		//log.Println("Waiting")
		<-doneWrite[i]
		//log.Println("Finally Moving Forward")
	}
	//log.Printf("Returning from Shuffle Thread")

	done <- true
}

//Async write routine. Don't be fooled by the simplicity this is complicated.
//Each routine is pinned to a TCP connection. Upon reciving a segment on an
//async channel this function writes a region of memory referenced by the
//segment to indexed host.
func WriteRoutine2(writeTo chan FixedSegment, doneWriting chan bool, conn net.Conn) {

	defer conn.Close()
	for {
		seg := <-writeTo
		//log.Printf("Received Writing Segement of size %d", seg.n)
		buf := (*[SORTBUFBYTESIZE]byte)(unsafe.Pointer(seg.buf))
		_, err := conn.Write(buf[:seg.n])
		if err != nil {
			//log.Println(err)
			log.Fatal(err)
		}
		//Just Return

		//log.Printf("Returning Control for seg of size %d", seg.n)
		doneWriting <- true
	}
}

func ListenRoutine(readDone chan Segment, progress chan bool, remoteHostIndex int, hostname string, hostIpMap map[string]string, indexMap map[string]int, ports [][]int) {

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
	defer conn.Close()

	var total int64
	start := time.Now()
	conn.SetReadDeadline(time.Now().Add(10000 * time.Second))
	for {
		//This is going to cause a bug TODO total will overflow rbufs
		n, err := conn.Read(rbufs[remoteHostIndex])
		total += int64(n)
		//check For a termination Condition
		if err != nil {
			readDone <- Segment{offset: -1, n: -1, index: remoteHostIndex}
			break
		}

		if detectTCPClose(conn) {
			break
		}
		filelock.Lock()
		f.WriteString(fmt.Sprintf("%0.3f,%d\n", time.Now().Sub(start).Seconds(), total))
		filelock.Unlock()
		readDone <- Segment{offset: total, n: int64(n), index: remoteHostIndex}
		<-progress
	}
}

func writeOutputToFile(sorted []int, hostIndex int) {
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
	c.SetReadDeadline(time.Now())
	if _, err := c.Read(one); err == io.EOF {
		log.Printf("%s detected closed LAN connection", "ID")
		c.Close()
		c = nil
		return true
	} else {
		c.SetReadDeadline(time.Now().Add(60 * time.Second))
		return false
	}
}

//This is a convience function for printing the amount of data being sorted on a single host
func totaldata() string {
	return fmt.Sprintf("%dMB", totaldataVal())
}

//RETURN THE TOTAL AMMOUNT OF DATA IN MB
func totaldataVal() int {
	return (((INTEGERS * 32) / 1024) / 1024)
}
