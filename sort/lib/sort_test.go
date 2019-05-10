package lib

import (
    "testing"
    "math/rand"
    "sort"
    )

const NUMITEMS = 100
var start = make(chan bool, 1)
var stop = make(chan bool, 1)

func PreambleSort(size int) ([]Item, []uint32) {
        var items = make([]Item,size)
        var ints = make([]uint32,size)
        for i:= range ints {
            ints[i] = uint32(i)
        }
        var r = rand.New(rand.NewSource(0))
        start <-true
        Randomize(items,r,start,stop)
        <-stop
        return items, ints

}

func TestQSort1(t * testing.T) {testQSort(1,t)}
func TestQSort1000(t * testing.T) {testQSort(1000,t)}
func TestQSort1000000(t * testing.T) {testQSort(1000000,t)}
func TestQSort10000000(t * testing.T) {testQSort(10000000,t)}

func testQSort(inputSize int, t *testing.T){
        items, ints := PreambleSort(inputSize)
        d := DirRange{items: &items, ints: &ints}
        sort.Sort(d)
        for i := 0;i < len(items)-1;i++ {
            if !d.Less(i,i+1) {
                t.Errorf("Items Not Sorted [%d,%d] -> (%x, %x)",i,i+1, items[ints[i]].Key[:],items[ints[i+1]].Key[:])
            }
        }
        return 
}

//func BenchmarkQSort1(b* testing.B) {benchmarkQSort(1,b)}
func BenchmarkQSort1000(b* testing.B) {benchmarkQSort(1000,b)}
//func BenchmarkQSort1000000(b* testing.B) {benchmarkQSort(1000000,b)}

func benchmarkQSort(inputSize int, b * testing.B) {
    for t := 0; t < b.N; t++ {
        items, ints := PreambleSort(inputSize)
        sort.Sort(DirRange{items: &items, ints: &ints})
     }
}
