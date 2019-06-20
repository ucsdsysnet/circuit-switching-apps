package lib

import (
    "fmt"
//    "log"
    "unsafe"
    "math/rand"
)

const BUFPAGES = 4
const SORTBUFSIZE = (4096 * BUFPAGES) / ITEMSIZE
const KEYSIZE = 10
const VALUESIZE = 90

//const VALUESIZE = 90
const ITEMSIZE = KEYSIZE + VALUESIZE

type Item struct {
	Value [VALUESIZE]byte
	Key   [KEYSIZE]byte
}

func (i *Item) Size() int {
	return len(i.Key) + len(i.Value)
}

func (i *Item) String() string {
    /*
	s := fmt.Sprintf("Key[")
    for j := range i.Key {
        s += fmt.Sprintf("%x,",i.Key[j])
    }
	s += fmt.Sprintf("]:Value[")
    for j := range i.Value {
        s += fmt.Sprintf("%x,",i.Value[j])
    }
    s += fmt.Sprintf("]")
    return se(
    */
    
    return fmt.Sprintf("Key[%X] Value[%X]",i.Key, i.Value)
}

type DirRange2 []Item

type DirRange struct{
   Items *[]Item
    Ints *[]uint32
}

type DirRange3 struct {
    Ints *[]uint32
    Keys *[][KEYSIZE]byte
}



func Randomize(data []Item, r *rand.Rand, start, stop chan bool) {
	var datalen = len(data)
    var randItem Item
    r.Read(randItem.Value[0:VALUESIZE])
	<-start
    //fmt.Printf("Data Length %d\n",datalen)
    //fmt.Printf("Modified len %d\n", datalen*ITEMSIZE)
    //r.Read((*itembuf)[0:datalen])
	for i := 0; i < datalen; i++ {
		r.Read(data[i].Key[0:KEYSIZE])
		data[i].Value = randItem.Value
	}
    //var itembuf *[]byte
    //itembuf = (*[]byte)(unsafe.Pointer(&data))
    //r.Read((*itembuf)[0:datalen])
	stop <- true
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
	N        int64
}

//Fixed segments are preallocated structs of data used for copying to the network.
type FixedSegment struct {
	Buf *[SORTBUFSIZE]Item
	N   int
}


func (a DirRange2) Key(i int) []byte {return a[i].Key[:]}
func (a DirRange2) Len() int {return len(a)}
func (a DirRange2) Swap(i, j int) {
	for k := range a[i].Key {
		a[i].Key[k], a[j].Key[k] = a[j].Key[k], a[i].Key[k]
	}
	for k := range a[i].Value {
		a[i].Value[k], a[j].Value[k] = a[j].Value[k], a[i].Value[k]
	}
}
func (a DirRange2) Less(i, j int) bool {
    var k int
    for k=0;k<KEYSIZE;k++{
        if uint8(a[i].Key[k]) != uint8(a[j].Key[k]) {
            return uint8(a[i].Key[k]) < uint8(a[j].Key[k])
        }
    }
    return true
}




func (a DirRange) Key(i int) uint64 { 
    return *(*uint64)(unsafe.Pointer(&(*a.Items)[i].Key))
}

func (a DirRange) Len() int { 
    if len(*a.Ints) < len(*a.Items) {
        return len(*a.Ints)
    } else {
        return len(*a.Items)
    }
}
func (a DirRange) Swap(i, j int) {
    (*a.Ints)[i], (*a.Ints)[j] = (*a.Ints)[j], (*a.Ints)[i]
}

const compsize = 8

func (a DirRange) Less(i, j int) bool {
    
    var ib *[KEYSIZE]byte
    var jb *[KEYSIZE]byte
    ib = &(*a.Items)[(*a.Ints)[i]].Key
    jb = &(*a.Items)[(*a.Ints)[j]].Key
    return Less(ib, jb)

}

func (a DirRange3) Len() int {
    if len(*a.Ints) < len(*a.Keys) {
        return len(*a.Ints)
    } else {
        return len(*a.Keys)
    }
}

func (a DirRange3) Swap(i, j int) {
    (*a.Ints)[i], (*a.Ints)[j] = (*a.Ints)[j], (*a.Ints)[i]
}

func (a DirRange3) Less(i, j int) bool {
    var ib *[KEYSIZE]byte
    var jb *[KEYSIZE]byte
    ib = &(*a.Keys)[(*a.Ints)[i]]
    jb = &(*a.Keys)[(*a.Ints)[j]]
    return Less(ib, jb)

}

func Less(ib, jb  *[KEYSIZE]byte) bool {
    var ival uint64
    var jval uint64

    ival |= uint64((*ib)[7]) << uint64(0)
    ival |= uint64((*ib)[6]) << uint64(8)
    ival |= uint64((*ib)[5]) << uint64(16)
    ival |= uint64((*ib)[4]) << uint64(24)
    ival |= uint64((*ib)[3]) << uint64(32)
    ival |= uint64((*ib)[2]) << uint64(40)
    ival |= uint64((*ib)[1]) << uint64(48)
    ival |= uint64((*ib)[0]) << uint64(56)

    jval |= uint64((*jb)[7]) << uint64(0)
    jval |= uint64((*jb)[6]) << uint64(8)
    jval |= uint64((*jb)[5]) << uint64(16)
    jval |= uint64((*jb)[4]) << uint64(24)
    jval |= uint64((*jb)[3]) << uint64(32)
    jval |= uint64((*jb)[2]) << uint64(40)
    jval |= uint64((*jb)[1]) << uint64(48)
    jval |= uint64((*jb)[0]) << uint64(56)

    if ival != jval {
        return ival < jval
    } else {
        //Else do the rest
        ival = uint64((*ib)[8]) << uint64(0)
        ival = uint64((*ib)[9]) << uint64(8)
        jval |= uint64((*jb)[8]) << uint64(0)
        jval |= uint64((*jb)[9]) << uint64(8)
        return ival < jval
    }

}
