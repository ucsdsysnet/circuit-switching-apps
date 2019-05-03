package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/pkg/profile"
	"io"
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
	PerformSort      = true
	ConnectToPeers    = true
	WriteOutputToFile = false
	AsyncWrite        = true
	SPEEDTEST         = false
	PROFILE           = true
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

//command line usage
var usage = "main.go hostname hosts.txt"

//The total number of integers to generate and sort per node

//This is the max on 128GB machines at 10:90 kv's
//const ITEMS = 620000000
//This is a practial number which allows for nearly full memory on 128GB machines
const ITEMS = 500000000

const TOTALCORES = 40
const SHUFFLECORES = 4
const BUFPAGES = 4
const SORTBUFSIZE = (4096 * BUFPAGES) / ITEMSIZE

const SORTBUFBYTESIZE = SORTBUFSIZE * ITEMSIZE
const SORTTHREADS = TOTALCORES

const SHUFFLERTHREADS = SHUFFLECORES

const RECTHREADS = 1

const RANDTHREADS = TOTALCORES
const MAXHOSTS = 8
const BALLENCERATIO = 1.5


//Constants for determining the largest integer value, used to ((aproximatly))
//evenly hash integer values across hosts)
const MaxUint = ^uint64(0)

//Time to sleep after calling read on a TCP socket to allow the connection to
//properly listen
const READWAKETIME = 2

//Time to sleep after connecting to a remote TCP socket to allow the connection
//to be established
const WRITEWAKETIME = 2

//Read timeout during sort, this is the MAX amount of time before a host will
//wait before beginning shuffle. If there is too much data to sort in <
//READTIMEOUT time adjust this timeout
const READTIMEOUT = 30

//As the name would suggest this is the tcp read timeout after shuffle has begun
const READTIMEOUTSHUFFLESTARTED = 1

const KEYSIZE = 10
const VALUESIZE = 90

//const VALUESIZE = 90
const ITEMSIZE = KEYSIZE + VALUESIZE

//Each host has SORTTHREAD connections with each other host. Each connection is
//offset by PORTOFFSET
const PORTOFFSET = 1000

//ABSOLUTE TIMERS
//Absolute time to begin the shuffle operation (~30s for 128 GB machines)
const SHUFFLESTART = 30
// Set to 74 for 10G, and X for 40G
const SORTSTART = 74
//const SORTSTART = 43

type Item struct {
	Value [VALUESIZE]byte
	Key   [KEYSIZE]byte
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
//NOTE Second itteration. There is no need to do anything with offsets, or
//indexes. It does not matter who sent the data, all that matters is the thread
//that it is going to.
//TODO Segment has been largely deprecated and is barely used except to signal
//that a channel is done reading.
type Segment struct {
	n        int64
}

//Fixed segments are preallocated structs of data used for copying to the network.
type FixedSegment struct {
	buf *[SORTBUFSIZE]Item
	n   int
}

var recBufLock sync.Mutex

var tLock = make([]sync.Mutex, SORTTHREADS)

//base port all others are offset from this by hostid * hosts + hostid
var basePort = 11600

//All generated integers, some will be sent, some are sorted locally
var toSend = make([]Item, ITEMS)

//To sort are the integers which will be sorted locally. Remote values for this
//node are eventually placed into toSort
//var toSort = make([]Item, ITEMS*BALLENCERATIO)
var toSort = make([][]byte, SORTTHREADS)

//Abs start begins at the beginning of the sort and is used to calculagte
//absolute timeouts.
var AbsStart time.Time
//Sort start is used to start sorting at an absolute time.
var SortStart time.Time

var shuffleStarted = false

func main() {
	AbsStart = time.Now()
	flag.Parse()
	//cpu profileing

	//Remove filename from arguments
	args := os.Args[1:]
	//Parse arguments
	hostname := args[0]

    //Generate a map of hostnames -> ip and hostname -> host index // TODO this
    //is slow index with arrays in the future
	ipMap, indexMap := parseHosts(args[1])

    // 2-D grid of port pairs arranged so the sender is the first index, and
    // the receiver the second
	// [ [0,0] [0,1] ]  // [ 0 1 ]
	// [ [1,0] [1,1] ]  // [ 2 3 ]
	// for an index like [1,0] it reads host1 connects to host0 on that port ->
	// host 0 will be listening on it
	ports := getPortPairs(ipMap)
	log.SetPrefix("[" + hostname + "] ")

	//Setup corresponding sort threads
	for i := 0; i < SORTTHREADS; i++ {
		toSort[i] = make([]byte, (ITEMS*ITEMSIZE*BALLENCERATIO)/SORTTHREADS)
	}

	writeTo := make([][]chan FixedSegment, len(ipMap))
	readDone := make(chan Segment, 1)
	writeDone := make([][]chan bool, len(ipMap))
	//Alloc an array of outgoing channels to write to other hosts.
	for i := 0; i < len(ipMap); i++ {
		writeTo[i] = make([]chan FixedSegment, SORTTHREADS)
		writeDone[i] = make([]chan bool, SORTTHREADS)
		for j := 0; j < SORTTHREADS; j++ {
			writeTo[i][j] = make(chan FixedSegment, 3)
			writeDone[i][j] = make(chan bool, 3)
		}
	}

    //ThreadSortCount, counts the number of Items that each sorting thread must
    //sort. This number is incremented by multiple threads. If there are 8
    //hosts, then there will be 8 threads contesting the thread sort count for
    //a given sort thread. This makes this approach unscalable for hundreds of
    //hosts. threadSortCount is also used to determine at which offset a given
    //receiving thread should writ to the ToSort array. Currently all access to
    //threadSortCount is protected by the tLock array.
	var threadSortCount = make([]int, SORTTHREADS)

	if ConnectToPeers {
		//Launch TCP listening threads for each other host (one way tcp for ease)
		for remoteHost, remoteIndex := range indexMap {
			//Don't connect with yourself silly!
			if indexMap[hostname] == indexMap[remoteHost] {
				continue
			}
			for i := 0; i < SORTTHREADS; i++ {
				go ListenRoutine(readDone, remoteIndex, hostname, ipMap, indexMap, ports, i, &threadSortCount)
			}
		}

		//Sleep so that all conections get established TODO if a single host
		//does not wake up the sort breaks: develop an init protocol for safety
		time.Sleep(time.Second * READWAKETIME)

		for remoteHost, remoteAddr := range ipMap {
			//Don't connect with yourself you have your integers!
			if indexMap[hostname] == indexMap[remoteHost] {
				continue
			}

			for i := 0; i < SORTTHREADS; i++ {
		        thisHostId := indexMap[hostname]
				remoteHostIndex := indexMap[remoteHost]
				remotePort := ports[thisHostId][remoteHostIndex] + basePort + (i * PORTOFFSET)
				conn, err := net.Dial("tcp4", fmt.Sprintf("%s:%d", remoteAddr, remotePort))
				if err != nil {
					log.Fatalf("Unable to connect to remote host %s on port %d : Error %s", remoteHost, remotePort, err)
				}
				defer conn.Close()
				//Launch writing thread

				go WriteRoutine2(writeTo[remoteHostIndex][i], writeDone[remoteHostIndex][i], conn)
			}
		}
	}

	time.Sleep(time.Second * WRITEWAKETIME)

	totalHosts := len(ipMap)
	GenRandomController(totalHosts)

	//Start Listening

	//Start the profile after generating the random data
	if PROFILE {
		defer profile.Start().Stop()
		profile.ProfilePath(*cpuprofile)
	}

	//Wait for transmission of other hosts to end, and count the number of
	//bytes transmitted by othere hosts to perform a single decode operation

	//TODO I'm not sure this is 100 percent safe, in some cases an error could
	//occur, and the end transmission might be triggered (this is a pragmatic
	//approach to get the sort done, just restart if there is a crash.

	//Now each of the reads should be coppied back into a shared buffer, to begin lets use a new toSort
	var doneHosts int
	var totalRead int

	log.Println("Launching a read terminating thread")
	var doneReading = make(chan bool, 1)

	for i := 0; i < RECTHREADS; i++ {
		go asyncRead(doneReading, readDone,  &doneHosts, &toSort, &threadSortCount, &totalRead, totalHosts, indexMap[hostname])
	}

	for {
		//course syncronization
		//TODO syncronize with proper messages
		if time.Since(AbsStart) > (time.Second * SHUFFLESTART) {
			break
		}
	}

	//Start Shuffling
	log.Println("Started Shuffle")
	SortStart = time.Now()
	go ShuffleController(totalHosts, &threadSortCount, hostname, indexMap, writeTo, writeDone, readDone)



	for {
		//course syncronization
		//TODO syncronize with proper messages
		if time.Since(AbsStart) > (time.Second * SORTSTART) {
			break
		}
	}
    if false {
        //\HASHING CODE
        shuffleStarted = true
        for i := 0; i < RECTHREADS; i++ {
            <-doneReading
            log.Printf("Rec Thread [%d] Complete", i)
        }
    }

	if PerformSort {
		startSorting := make(chan bool, 1)
		stopSorting := make(chan bool, 1)
		log.Println("Starting Sort!!")
		SortController(&toSort, threadSortCount, startSorting, stopSorting)
		log.Println("Sort Complete!!!")
	}

	if WriteOutputToFile {
		log.Println("Writing to file")
		writeOutputToFile(&toSort, indexMap[hostname])
		log.Println("Write Complete")
	}
}

func SortController(toSort *[][]byte, threadRanges []int, sortStart chan bool, sortDone chan bool) {
    var itempointer *[]Item
	for i := 0; i < SORTTHREADS; i++ {
        itempointer = (*[]Item)(unsafe.Pointer(&((*toSort)[i])))
		go Sorter((*itempointer)[0:(threadRanges[i]/ITEMSIZE)], sortStart, sortDone)
	}
	for i := 0; i < SORTTHREADS; i++ {
		sortStart <- true
	}
	for i := 0; i < SORTTHREADS; i++ {
		<-sortDone
	}
}

func Sorter(toSort []Item, sortStart chan bool, sortDone chan bool) {
	<-sortStart
	sort.Sort(DirRange(toSort))
	sortDone <- true
}

func asyncRead(
	readchan chan bool,
	readDone chan Segment,
	doneHosts *int,
	toSortL *[][]byte,
	threadIndex *[]int,
	sortIndex *int,
	hosts int,
	hostid int) {

	var total int64
	started := false
	readingTime := time.Now()
	var seg Segment

	doneTimeout := time.After(time.Second)
	for {
		if !started && total > 10000 {
			started = true
			readingTime = time.Now()
		}
		select {
		case seg = <-readDone:
			total += seg.n

			if seg.n == -1 {
				recBufLock.Lock()
				(*doneHosts)++
				recBufLock.Unlock()
			}

		case <-doneTimeout:
			if (*doneHosts) >= ((hosts - 1) * SORTTHREADS) {
				log.Printf("Exiting reading thread")
				readingTimeTotal := time.Since(readingTime)
				log.Printf("Done Reading data from other hosts in %0.3f seconds rate = %0.3fMB/s, total %dMB\n",
					readingTimeTotal.Seconds(),
					float64(((total*8)/(1024*1024)))/readingTimeTotal.Seconds(),
					(total*8)/(1024*1024))
				readchan <- true
				return
			}
			doneTimeout = time.After(time.Second * 1)
		}
	}

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
	log.Printf("Total Hosts %d", totalHosts)
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
	dataGenTimeTotal := time.Since(dataGenTime)
	log.Printf("Done Generating DataSet in %0.3f seconds rate = %0.3fMB/s\n",
		dataGenTimeTotal.Seconds(),
		float64(totaldataVal())/dataGenTimeTotal.Seconds())
}

/*
* The shuffler method distributes key across hosts. values from the data array
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
	localSortIndexRef *[]int,
	hosts int,
	threadIndex int,
	myIndex int,
	start,
	stop chan bool,
	writeTo [][]chan FixedSegment,
	doneWrite [][]chan bool,
	readDone chan Segment,
) {

	var (
		outputBuffers [MAXHOSTS * SORTTHREADS][SORTBUFSIZE]Item
		hostIndex     [MAXHOSTS * SORTTHREADS]int
	)
	var sorteeHost int
	var sorteeThread int
	var masterIndex int

    var baseOffset int
	var datalen = len(data)
	var uintkey uint64
	var hQuant = uint64((MaxUint / uint64(hosts)))
	var tQuant = (uint64((MaxUint / uint64(hosts)))) / SORTTHREADS

	//TODO kill
	<-start

	//The next step forward is to keep all local data in the same buffer. To do
	//this data in the input buffer should be overwritten with local data
	//rather than overwriting toSort. The modification goes as thus.

	//1) Each thread write to it's own pre area of the shared data with a variable localSort index



	for i := 0; i < datalen; i++ {

		uintkey = uint64(data[i].Key[0]) << 56
		uintkey |= uint64(data[i].Key[1]) << 48
		uintkey |= uint64(data[i].Key[2]) << 40

        sorteeHost = int(uintkey / hQuant)
	    sorteeThread = int((uint64(uintkey) - uint64(uint64(sorteeHost) * hQuant)) / tQuant)
		masterIndex = int(sorteeHost * SORTTHREADS + sorteeThread)

		outputBuffers[masterIndex][hostIndex[masterIndex]] = data[i]
		hostIndex[masterIndex]++
		//Buffer full time to send
		if hostIndex[masterIndex] == SORTBUFSIZE {


			hostIndex[masterIndex] = 0
			if myIndex == sorteeHost {

                tLock[sorteeThread].Lock()
                baseOffset = (*localSortIndexRef)[sorteeThread]
                (*localSortIndexRef)[sorteeThread] += SORTBUFBYTESIZE
                tLock[sorteeThread].Unlock()
                
                copy(toSort[sorteeThread][baseOffset:(baseOffset + SORTBUFBYTESIZE)],(*[SORTBUFBYTESIZE]byte)(unsafe.Pointer(&outputBuffers[masterIndex]))[:SORTBUFBYTESIZE])

			} else {
				writeTo[sorteeHost][sorteeThread] <- FixedSegment{buf: &outputBuffers[masterIndex], n: SORTBUFSIZE}
				if !AsyncWrite {
					<-doneWrite[sorteeHost][sorteeThread]
				}
			}
		}
	}

	for i := 0; i < hosts; i++ {
		if i == myIndex {
			for j := 0; j < SORTTHREADS; j++ {


				if hostIndex[i*(SORTTHREADS)+j] == 0 {
					continue
				}

                tLock[sorteeThread].Lock()
                baseOffset = (*localSortIndexRef)[sorteeThread]
                (*localSortIndexRef)[sorteeThread] += SORTBUFBYTESIZE
                tLock[sorteeThread].Unlock()
                
                copy(toSort[sorteeThread][baseOffset:(baseOffset + SORTBUFBYTESIZE)],(*[SORTBUFBYTESIZE]byte)(unsafe.Pointer(&outputBuffers[i*(SORTTHREADS) + j]))[:SORTBUFBYTESIZE])
			}
		} else {
			for j := 0; j < SORTTHREADS; j++ {
				if hostIndex[i*(SORTTHREADS)+j] == 0 {
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

				writeTo[i][j] <- FixedSegment{buf: &outputBuffers[i*(SORTTHREADS)+j], n: SORTBUFSIZE}
                //Send zero to close the channel
				writeTo[sorteeHost][sorteeThread] <- FixedSegment{buf: &outputBuffers[masterIndex], n: 0}
				if !AsyncWrite {
					<-doneWrite[i][j]
				}
			}
		}

	}
	stop <- true
}

func ShuffleController(totalHosts int,
	localSortedCounter *[]int,
	hostname string,
	indexMap map[string]int,
	writeTo [][]chan FixedSegment,
	writeDone [][]chan bool,
	readDone chan Segment) {

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
		go shuffler(toSend[min:max], localSortedCounter, totalHosts, i, indexMap[hostname], startShuffle, stopShuffle, writeTo, writeDone, readDone )
	}

	//HASHING CODE
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

	dataHashTimeTotal := time.Since(dataHashTime)
	log.Printf("Done Hashing Data across hosts in %0.3f seconds rate = %0.3fMB/s\n",		dataHashTimeTotal.Seconds(),
		float64(totaldataVal())/dataHashTimeTotal.Seconds())
}

//Async write routine. Don't be fooled by the simplicity this is complicated.
//Each routine is pinned to a TCP connection. Upon reciving a segment on an
//async channel this function writes a regi{
//on of memory referenced by the
//segment to indexed host.
func WriteRoutine2(writeTo chan FixedSegment, doneWriting chan bool, conn net.Conn) {
	var seg FixedSegment
	var err error
    var doneCounter int
	for {

		seg = <-writeTo
        //TODO move below the write (may save some time)
        if seg.n == 0 {
            doneCounter++
            if doneCounter >= SHUFFLERTHREADS {
                conn.Close()
                return
            }
        }

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
other host over a TCP connection. This function starts a TCP listen, waits for
a connection to start and then begins reading. As of May 3 2019 a listen
routine exists for each SortThread host pair. Data is read directly into the
ToSort array based on indexs determined by *threadCounter, a structure shared
between other listen routines, and Shuffle threads.

readDone - Channel of Segments, sends data  back to main application, and signals when the computation is over
remoteHostIndex - The remote host being read from
hostname - this hosts hostname
hostIpMap - maps hosts to IP used for connecting and debugging
indexMap - relates hosts to their global index in the configuration file
ports - port matrix, used to determine which port to listen on.
hostConnection - the nth connection that is being made to the particular host bounded by CONSPERHOST
threadCounter - index into the ToSort array, prior to receving threadCounter is used to determine the offset for this Listen routine
*/
func ListenRoutine(
    readDone chan Segment,
	remoteHostIndex int,
	hostname string,
	hostIpMap map[string]string,
	indexMap map[string]int,
	ports [][]int,
	hostConnection int,
    threadCounter *[]int,
) {

	var total int64
	var n int
	var err error

	var seg Segment
    var baseOffset int
    var ittBaseOffset int
    var completeRecBuf = 1

	//TODO for clenliness put this into its own function
	//This part is special. In the future there should be a big block of mmapped memory for each of the listen routines
	thisHostId := indexMap[hostname]

	//reminder if a -> b than  b listens on ports [a][b]
	port := ports[remoteHostIndex][thisHostId] + basePort + (PORTOFFSET * hostConnection)
	ln, err := net.Listen("tcp4", fmt.Sprintf("%s:%d", hostIpMap[hostname], port))
	if err != nil {
		log.Fatal(err)
	}
	//log.Printf("Listen Complete\n")
	conn, err := ln.Accept()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(10000 * time.Second))
	for {
            tLock[hostConnection].Lock()
            baseOffset = (*threadCounter)[hostConnection]
            ittBaseOffset = baseOffset
            completeRecBuf++
            (*threadCounter)[hostConnection] += SORTBUFBYTESIZE
            tLock[hostConnection].Unlock()

            for ittBaseOffset < baseOffset + SORTBUFBYTESIZE {
                n, err = conn.Read(toSort[hostConnection][ittBaseOffset:(baseOffset + SORTBUFBYTESIZE)])
                ittBaseOffset += n
                total += int64(n)
                if n == 0 {
                    break
                }
            }

            if detectTCPClose(conn) {
                seg.n = -1
                readDone <- seg
                //give buf away
                break
            }

            if err != nil {
                //log.Fatalf("Read Error %s. Did the connection die, or did one of the hosts crash? Generic errors should be handeled", err.Error())
                break //TODO the key control break, relies on getting a tcp timeout
            }

            if total == int64(len(toSort[hostConnection])) {
                log.Println(
                    "WARNING. This should probably be a fatal error but lets not panic for now."+
                    "The toSort array is overfilled which means the data was skewed many std's away from random."+
                    "This sort is intended for random input so this error should not occur. -Stew (May 3 2019)")
                break

            }
	}

    tLock[hostConnection].Lock()
    (*threadCounter)[hostConnection] = int(total)
    tLock[hostConnection].Unlock()

	seg.n = -1
	readDone <- seg
}

func writeOutputToFile(sorted *[][]byte, hostIndex int) {
	doneWrite := make(chan bool, 1)
    var itempointer *[]Item
	for i := range (*sorted) {
        itempointer = (*[]Item)(unsafe.Pointer(&((*sorted)[i])))
        filename := fmt.Sprintf("%d_%d.sorted", hostIndex,i)
		go WriteOutputToSingleFile(itempointer, filename, doneWrite)
	}
	for _ = range (*sorted) {
		<-doneWrite
	}
}

func WriteOutputToSingleFile(sorted *[]Item, filename string, doneWrite chan bool) {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Unable to open output file %s : Error %s", filename, err.Error())
	}
	for i := range (*sorted) {
		f.WriteString((*sorted)[i].String() + "\n")
	}
	doneWrite <- true
}

//Returns a map of hostnames to Ip address, and hostname to host index based on
//a config file
func parseHosts(configFile string) (map[string]string, map[string]int) {
	hostMap := make(map[string]string)
	idMap := make(map[string]int)
	f, err := os.Open(configFile)
	if err != nil {
		log.Fatalf("Unable to read in config file Error:%s\n -- %s", err,usage)
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
			log.Fatalf("Unable to parse host identifier %s to an integer: Error %s -- %s ", hostArgs[2], err, usage)
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


//This function returns true if the tcp connection has been closed, false otherwise
func detectTCPClose(c net.Conn) bool {
	one := []byte{}
	_, err := c.Read(one)
	if err == nil {
        if !shuffleStarted {
		    c.SetReadDeadline(time.Now().Add(time.Second * READTIMEOUT))
        } else {
		    c.SetReadDeadline(time.Now().Add(time.Second * READTIMEOUTSHUFFLESTARTED))
        }
            
		return false
	} else if err == io.EOF {
		log.Printf("%s detected closed LAN connection", "ID")
		c.Close()
		c = nil
		return true
	} else {
        if !shuffleStarted {
		    c.SetReadDeadline(time.Now().Add(time.Second * READTIMEOUT))
        } else {
		    c.SetReadDeadline(time.Now().Add(time.Second * READTIMEOUTSHUFFLESTARTED))
        }
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
}
func (a DirRange) Less(i, j int) bool {
    //var k int
    //var length = len(a[i].Key)
    var ival uint64
    var jval uint64
    ival = *(*uint64)(unsafe.Pointer(&a[i].Key))
    jval = *(*uint64)(unsafe.Pointer(&a[j].Key))
    return ival < jval
}
