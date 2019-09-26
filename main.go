package main

import (
	"container/list"
	"flag"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

const (
	// Database max request size key in storage
	DB_REQ_SIZE_KEY = "DB_REQ_SIZE"

	// Prefixes for data and expiration time
	DB_DATA_PREFIX       = "0"
	DB_EXPIRATION_PREFIX = "1"

	// Default max request size
	DEFAULT_REQ_SIZE = 256

	// Supported operations
	READ   = "READ"
	PUT    = "PUT"
	DELETE = "DELETE"
)

type Application struct {
	db *leveldb.DB

	workerCount     int
	maxSize         int
	storageCommands []string
	requests        RequestList
	sync.Mutex
	lockVal map[string]struct{}
}

// Request queue storing/controlling
type RequestList struct {
	sync.Mutex
	list list.List
}

type Request struct {
	conn           net.Conn
	connectionTime time.Time
	command        Command
}

type Command struct {
	argCommand        string
	argKey            []byte
	argValue          []byte
	argExpirationTime time.Time
}

// Lock value in case value gets modified
func LockValue(app *Application, key string) bool {
	app.Lock()
	defer app.Unlock()
	if _, present := app.lockVal[key]; present {
		return false
	}
	app.lockVal[key] = struct{}{}
	return true
}

func UnlockValue(app *Application, key string) {
	app.Lock()
	delete(app.lockVal, key)
	app.Unlock()
}

// Removing expired value
func ExpireRecord(app *Application, expirationKey, valueKey []byte) {
	LockValue(app, string(valueKey[1:]))
	batch := new(leveldb.Batch)
	batch.Delete(expirationKey)
	batch.Delete(valueKey)
	UnlockValue(app, string(valueKey[1:]))
	err := app.db.Write(batch, nil)
	if err != nil {
		LogE(err.Error())
	}
}

// Request adding to queue
func AddRequest(app *Application, req Request) {
	app.requests.Lock()
	app.requests.list.PushBack(req)
	app.requests.Unlock()
}

// Pulling request from the queue
func PullRequest(app *Application) Request {
	reqElement := app.requests.list.Front()
	req := reqElement.Value.(Request)
	app.requests.list.Remove(reqElement)
	return req
}

// Common log printing
func printLog(level, msg string) {
	fmt.Printf("%s /%s: %s\n", time.Now().Format("01-02-2006 15:04:05"), level, msg)
}

// Log on error level
func LogE(msg string) {
	printLog("Error", msg)
}

// Log on info level
func LogI(msg string) {
	printLog("Info", msg)
}

// Max request value can also be stored in database
func getMaxRequestSize(db *leveldb.DB) int {
	value, err := db.Get([]byte("DB_REQ_SIZE"), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			value = []byte(strconv.Itoa(DEFAULT_REQ_SIZE))
			db.Put([]byte(DB_REQ_SIZE_KEY), value, nil)
		} else {
			panic(err)
		}
	}
	fmt.Printf("Max request size now is %s bytes\n", string(value))
	returnValue, err := strconv.Atoi(string(value))
	if err != nil {
		return DEFAULT_REQ_SIZE
	}
	return returnValue
}

// Checking if command belongs to special server commands
func (app *Application) CheckServerCommand(command Command) bool {
	for _, commandName := range app.storageCommands {
		if commandName == command.argCommand {
			app.db.Put([]byte(command.argCommand), command.argValue, nil)
			LogI(fmt.Sprintf("Command %s value is set to %s and will be changed after server restart.", string(command.argKey), string(command.argValue)))
			return true
		}
	}
	return false
}

// Test method
func listAllValues(app *Application) {
	iter := app.db.NewIterator(nil, nil)
	for iter.Next() {
		fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
	}
}

func main() {
	pPort := flag.Int("port", 2222, "TCP server port")
	path := flag.String("dbpath", "db", "Database path")
	pWorkerCount := flag.Int("workers", 10, "Workers count for repository communication")
	flag.Parse()
	workerCount := *pWorkerCount
	port := ":" + strconv.Itoa(*pPort)
	db, err := leveldb.OpenFile(*path, nil)
	fmt.Println("Print './master -h' if you want to see all params")
	fmt.Printf("Database path: %s\n", *path)
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	fmt.Println(`Usage:   
	'READ key' for reading value from repo  
	'PUT key *expiration time in seconds* value' to insert value into repo 
	'DELETE key' for deleting value by key`)
	maxSize := getMaxRequestSize(db)

	// Storage command slice
	storageCommands := []string{
		DB_REQ_SIZE_KEY,
	}

	a := Application{
		db:              db,
		maxSize:         maxSize,
		requests:        RequestList{},
		storageCommands: storageCommands,
		// Slice for locked values
		lockVal:     make(map[string]struct{}),
		workerCount: workerCount,
	}

	HandleAndServe(&a, port)
}
