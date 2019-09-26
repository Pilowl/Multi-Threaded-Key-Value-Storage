package main

import (
	"fmt"
	"math/rand"
	"net"
	"time"
)

const (
	TOTAL_REQUESTS = 300
	MIN_TIME       = 30
	MAX_TIME       = 90
)

var completedTests = 0
var succeeded = 0

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
	for i := 0; i < TOTAL_REQUESTS; i++ {
		go runTest(i)
	}

	for {
		if completedTests == TOTAL_REQUESTS {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	concludeResults()
}

func concludeResults() {
	fmt.Printf("Tests succeeded: %d of %d (%0.2f%%)", succeeded, TOTAL_REQUESTS, float64(succeeded)/float64(TOTAL_REQUESTS)*100)
}

func runTest(i int) {
	expirationTime := rand.Intn(MAX_TIME-MIN_TIME) + MIN_TIME

	conn, err := net.Dial("tcp", "localhost:2222")
	if err != nil {
		panic(err.Error())
	}
	conn.Write([]byte(fmt.Sprintf("PUT %d %d %s", i, expirationTime, "test")))
	buff := make([]byte, 256)
	n, _ := conn.Read(buff)
	conn.Close()
	fmt.Println(string(buff[:n]))

	time.Sleep(time.Duration(expirationTime) * (time.Second))

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
	fmt.Println("Found", string(buff[:n]), "Expected", msgNotExist)
	fmt.Println(i, "test completed")
	completedTests++
}
