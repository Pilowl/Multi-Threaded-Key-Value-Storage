package main

import (
	"fmt"
	"net"
	"time"
)

const (
	REQUEST_COUNT = 500
)

var completedTests int = 0
var totalPuttingTime int64 = 0
var totalReadingTime int64 = 0
var totalDeletingTime int64 = 0

var succeeded int = 0
var msgNotExist string

func main() {
	conn, err := net.Dial("tcp", "localhost:2222")
	if err != nil {
		panic(err.Error())
	}
	conn.Write([]byte(fmt.Sprintf("READ tasdadwqdqw")))
	buff := make([]byte, 256)
	n, _ := conn.Read(buff)
	conn.Close()
	msgNotExist = string(buff[:n])
	for i := 0; i < REQUEST_COUNT; i++ {
		time.Sleep(10 * time.Millisecond)
		go runTest(i)
	}
	for {
		if completedTests == REQUEST_COUNT {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	calculateResults()
}

func calculateResults() {
	fmt.Printf("Avg reading time: %f ms\n", float64(totalReadingTime)/float64(REQUEST_COUNT))

	fmt.Printf("Avg writing time: %f ms\n", float64(totalPuttingTime)/float64(REQUEST_COUNT))

	fmt.Printf("Avg deleting time: %f ms\n", float64(totalDeletingTime)/float64(REQUEST_COUNT))

	fmt.Printf("Succesfully processed records: %d/%d", succeeded, REQUEST_COUNT)
}

func runTest(i int) {
	conn, err := net.Dial("tcp", "localhost:2222")
	if err != nil {
		panic(err.Error())
	}
	start := time.Now()
	conn.Write([]byte(fmt.Sprintf("PUT %d 30 %s", i, string(time.Now().Unix()))))
	buff := make([]byte, 256)
	n, _ := conn.Read(buff)
	conn.Close()
	fmt.Println(string(buff[:n]))
	timeCalc := int64(time.Since(start) / time.Millisecond)
	fmt.Printf("Time writing: %d\n", timeCalc)
	totalPuttingTime += timeCalc

	conn, err = net.Dial("tcp", "localhost:2222")
	if err != nil {
		panic(err.Error())
	}
	start = time.Now()
	conn.Write([]byte(fmt.Sprintf("READ %d", i)))
	buff = make([]byte, 256)
	n, _ = conn.Read(buff)
	conn.Close()
	fmt.Println(string(buff[:n]))
	timeCalc = int64(time.Since(start) / time.Millisecond)
	fmt.Printf("Time reading: %d\n", timeCalc)
	totalReadingTime += timeCalc

	conn, err = net.Dial("tcp", "localhost:2222")
	if err != nil {
		panic(err.Error())
	}
	start = time.Now()
	conn.Write([]byte(fmt.Sprintf("DELETE %d", i)))
	buff = make([]byte, 256)
	n, _ = conn.Read(buff)
	conn.Close()
	fmt.Println(string(buff[:n]))
	timeCalc = int64(time.Since(start) / time.Millisecond)
	fmt.Printf("Time deleting: %d\n", timeCalc)
	totalDeletingTime += timeCalc

	conn, err = net.Dial("tcp", "localhost:2222")
	if err != nil {
		panic(err.Error())
	}
	conn.Write([]byte(fmt.Sprintf("READ %d", i)))
	buff = make([]byte, 256)
	n, _ = conn.Read(buff)
	conn.Close()
	if string(buff[:n]) == msgNotExist {
		succeeded++
	}

	fmt.Println(i, "thread completed")
	completedTests++
}
