package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var usage = "main.go hostname hosts.txt"

var transferFilename = "/home/stew/data/medium-nothing.dat"

const RTT = 5
const BUFSIZE = 1024 * 1024 * 5
const INTEGERS = 10000000

const MaxUint = ^uint(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

func getDestinationHost(value int, hosts int) int {
	dHost := value / ((MaxInt) / hosts)
	return dHost
}

const LITTLEBUFLEN = 128

type Segment struct {
	offset int64
	n      int64
	index  int
}

var buf = make([]byte, 0)
var rbufs [][]byte
var wbufs [][]byte
var ints [][]int
var basePort = 9020

var toSend = make([]int, 0)
var toSort = make([]int, 0)

func main() {
	args := os.Args[1:]
	fmt.Println(args)
	//Parse arguments
	hostname := args[0]
	ipMap, indexMap := parseHosts(args[1])
	ports := getPortPairs(ipMap)
	fmt.Println(indexMap)
	log.SetPrefix("[" + hostname + "] ")
	rbufs = make([][]byte, len(ipMap))
	wbufs = make([][]byte, len(ipMap))
	ints = make([][]int, len(ipMap))
	for i := range rbufs {
		//TODO be smart and exclude yourself from this buffer allocation: host i need no buffer for host i
		rbufs[i] = make([]byte, BUFSIZE)
		wbufs[i] = make([]byte, BUFSIZE)
		ints[i] = make([]int, 0)
	}

	//Launch Listening Threads for receving
	readDone := make(chan Segment, len(ipMap)-1)
	for remoteHost, remoteIndex := range indexMap {
		//Don't connect with yourself silly!
		if indexMap[hostname] == indexMap[remoteHost] {
			continue
		}
		//Fix problem with Index in the morning... I'm too tired.
		go ListenRoutine(readDone, remoteIndex, hostname, ipMap, indexMap, ports)
	}

	log.Printf("[%s] Sleeping for a moment to wait for other hosts to wake up", hostname)
	time.Sleep(time.Second * 5)
	//Now Start writing threads which will connect to the listeners
	writeTo := make([]chan Segment, len(ipMap))
	for i := 0; i < len(ipMap); i++ {
		writeTo[i] = make(chan Segment, 1)
	}

	thisHostId := indexMap[hostname]
	for remoteHost, remoteAddr := range ipMap {
		//Don't connect with yourself!
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
		go WriteRoutine(writeTo[remoteHostIndex], conn)

	}

	rand.Seed(int64(time.Now().Nanosecond()))

	//generate sorting datat
	for i := 0; i < INTEGERS; i++ {
		rvar := rand.Int()
		toSend = append(toSend, rvar)
	}

	//Determine which integers are going where
	for i := range toSend {
		sorteeHost := getDestinationHost(toSend[i], len(ipMap))
		if sorteeHost == indexMap[hostname] {
			//Don't send the data it belongs to you!
			//log.Printf("sorteeHost %d", sorteeHost)
			//log.Printf("%d goes to me!", toSend[i])
			toSort = append(toSort, toSend[i])
		} else {
			ints[sorteeHost] = append(ints[sorteeHost], toSend[i])
		}
	}

	//Broadcast The unsorted contents of an array of integers to other nodes
	for i := range ints {
		if indexMap[hostname] == i {
			continue
		}
		var buffer bytes.Buffer
		enc := gob.NewEncoder(&buffer)
		enc.Encode(ints[i])
		n, err := buffer.Read(wbufs[i])
		if err != nil {
			log.Fatal(err)
		}
		writeTo[i] <- Segment{offset: 0, n: int64(n), index: i}
	}

	//Wait for transmission of other hosts to end
	remoteBufCount := make([]int64, len(ipMap))
	doneHosts := make([]bool, len(ipMap))
	for {
		seg := <-readDone
		if seg.n == -1 {
			doneHosts[seg.index] = true
			if checkdone(doneHosts) {
				break
			}
		}
		remoteBufCount[seg.index] += seg.n
		//Sort your own stuff
		//Write own stuff to file
		//fmt.Printf("Keep On keeping On with %s", hostname)
		time.Sleep(time.Second)
	}

	//Time to Decode
	toDecode := make([]int, 0)
	for i := range rbufs {
		if indexMap[hostname] == i {
			//log.Printf("buf[%d] belongs to me\n", i)
			continue
		}
		decBuf := bytes.NewBuffer(rbufs[i][0:remoteBufCount[i]])
		dec := gob.NewDecoder(decBuf)
		dec.Decode(&toDecode)
		toSort = append(toSort, toDecode...)
	}
	sort.Ints(toSort)
	f, err := os.Create(fmt.Sprintf("%d.sorted", indexMap[hostname]))
	if err != nil {
		log.Fatal("Unable to open output file %s : Error %s")
	}
	for i := range toSort {
		f.WriteString(fmt.Sprintf("%d\n", toSort[i]))
	}

	log.Println("Sort Complete!!!")
	return
}

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

func readInFile(filename string) []byte {
	f, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("Unable to read in file %s ,Error : %s", filename, err)
	}
	return f
}

func detectTCPClose(c net.Conn) bool {
	one := []byte{}
	c.SetReadDeadline(time.Now())
	if _, err := c.Read(one); err == io.EOF {
		log.Printf("%s detected closed LAN connection", "ID")
		c.Close()
		c = nil
		return true
	} else {
		c.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
		return false
	}
}

func ListenRoutine(readDone chan Segment, remoteHostIndex int, hostname string, hostIpMap map[string]string, indexMap map[string]int, ports [][]int) {

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

	t := time.Now()
	prior := t
	var total int64 = 0
	//Sleep (5)

	for {
		n, err := conn.Read(rbufs[remoteHostIndex])
		//check For a termination Condition
		if err != nil {
			readDone <- Segment{offset: -1, n: -1, index: remoteHostIndex}
			break
			/*
				if detectTCPClose(conn) {
					break
				} else {
					continue
				}*/
		}
		total += int64(n)

		//Timing Block
		log.Printf("Received %d Bytes\n", n)
		current := time.Now()
		totalTime := current.Sub(t)
		fmt.Printf("Transfer %d Bytes in %d nanoseconds Rate = %d Btyes/ms\n",
			total,
			totalTime.Nanoseconds(),
			int64(n)/(current.Sub(prior).Nanoseconds()/(1000)),
		)
		if detectTCPClose(conn) {
			break
		}
		prior = current
		readDone <- Segment{offset: total, n: int64(n), index: remoteHostIndex}
	}
}

func WriteRoutine(writeTo chan Segment, conn net.Conn) {

	defer conn.Close()
	for {
		seg := <-writeTo
		//log.Println(seg)
		//fileData := readInFile(transferFilename)
		//fmt.Printf("Preparing to Send %s total bytes%d \n", wbufs[seg.index][seg.offset:seg.n], seg.n)
		conn.Write(wbufs[seg.index][seg.offset:seg.n])
		//Sleep (5)
	}
}
