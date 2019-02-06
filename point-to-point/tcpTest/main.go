package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	//"sort"
	"strconv"
	"strings"
	"time"
)

//command line usage
var usage = "main.go hostname hosts.txt"

//Size of byte buffers for reading and writing TODO this size should be larger for big sort sizes
const BUFSIZE = 4096

//The total number of integers to generate and sort per node
const WRITEBYTES = 1024 * 1024 * 100
const READBUFSIZE = 4096 * 32 // Page size
const READSAMPLE = 50
const NUMWRITES = 50

var (
	readbuf  [READBUFSIZE]byte
	writebuf [WRITEBYTES]byte
)

//base port all others are offset from this by hostid * hosts + hostid
var basePort = 10720

func main() {
	//Remove filename from arguments
	args := os.Args[1:]
	hostname := args[0]
	ipMap, indexMap := parseHosts(args[1])
	ports := getPortPairs(ipMap)
	log.SetPrefix("[" + hostname + "] ")

	epoch := time.Now().Unix() //For graphing purposes
	if len(ipMap) != 2 {
		log.Fatal("The purpose of this function is to benchmark golang TCP, supply only two hosts in the config")
	}
	var server, client bool

	if indexMap[hostname] == 0 {
		server = true
	} else if indexMap[hostname] == 1 {
		client = true
	}
	//Sanity Check
	if !(client || server) || (client && server) {
		log.Fatalf("Client and server are not mutually exclusive check the config %s", ipMap)
	}

	//Set up connecxtions

	if server {
		tcpReadFileName := "tcpReadBandwidth.dat"
		f, err := os.Create(tcpReadFileName)
		if err != nil {
			log.Fatal("Unable create %s", tcpReadFileName)
		}
		defer f.Close()
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", basePort+ports[0][1]))
		if err != nil {
			log.Fatal("Error listening for client! %s", err)
		}

		// accept connection on port
		conn, _ := ln.Accept()
		defer conn.Close()
		log.Println("Server Connected to Client -- Reading")
		// run loop forever (or until ctrl-c)
		var total int64
		var readRate float64
		var i int
		for {
			i++
			start := time.Now()
			n, _ := conn.Read(readbuf[:])
			total += int64(n)
			stop := time.Now()
			if i%READSAMPLE == 0 {
				readRate = ((float64(n) * 8) / (1024 * 1024)) / float64(stop.Sub(start).Seconds())
				output := fmt.Sprintf("%f,%f,%d\n", float64(stop.Unix()-epoch)+(float64(stop.Nanosecond())/float64(time.Second)), readRate, total/(1024*1024))
				//log.Print(output)
				f.WriteString(output)
			}
			if total >= (WRITEBYTES * NUMWRITES) {
				break
			}
		}
		log.Println("Server Finished")
	} else if client {
		//Dumb way to get server ip
		tcpWriteFileName := "tcpWriteBandwidth.dat"
		f, err := os.Create(tcpWriteFileName)
		if err != nil {
			log.Fatal("Unable create %s", tcpWriteFileName)
		}
		defer f.Close()

		var serverip string
		for i := range ipMap {
			if i != hostname {
				serverip = ipMap[i]
				break
			}
		}
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", serverip, basePort+ports[0][1]))
		if err != nil {
			log.Fatal("What are you doing? %s", err)
		}
		defer conn.Close()

		if err != nil {
			log.Fatal("What are you doing? Something went totally wrone in the network!")
		}

		var writeRate float64 // measured in MB's
		var total int64
		for i := 0; i < NUMWRITES; i++ {
			start := time.Now()
			conn.Write(writebuf[:])
			total += int64(len(writebuf))
			stop := time.Now()
			writeRate = float64(totalDataVal()) / float64(stop.Sub(start).Seconds())
			output := fmt.Sprintf("%f,%f,%d\n", float64(stop.Unix()-epoch)+(float64(stop.Nanosecond())/float64(time.Second)), writeRate, total/(1024*1024))
			//log.Print(output)
			f.WriteString(output)
		}
		log.Printf("Client Finished")
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
func totalData() string {
	return fmt.Sprintf("%dMB", totalDataVal())
}

//RETURN THE TOTAL AMMOUNT OF DATA IN MB
func totalDataVal() int64 {
	return (((WRITEBYTES * 8) / 1024) / 1024)
}
