package main

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func HandleAndServe(a *Application, port string) {
	l, err := net.Listen("tcp", port)
	if err != nil {
		LogE(err.Error())
		return
	}
	defer l.Close()
	LogI("Serving on port " + port[1:])

	// Starting garbage collector for temporary values
	go workerGarbageCollector(a)

	// Starting all workers
	for i := 0; i < a.workerCount; i++ {
		go workerProcess(a)
	}

	// Infinite loop for connection accepting, ctrl+c ftw
	for {
		c, err := l.Accept()
		if err != nil {
			LogE(err.Error())
			continue
		}
		// Request processing
		go HandleRequest(a, c)
	}
}

func workerGarbageCollector(app *Application) {
	for {
		// Iteration over all expiration time values
		iter := app.db.NewIterator(util.BytesPrefix([]byte(DB_EXPIRATION_PREFIX)), nil)
		for iter.Next() {
			// Starting from 17 to skip prefix + 16 bytes of checksum
			key := iter.Key()[1:11]
			timeUnix, _ := strconv.Atoi(string(key))
			expirationTime := time.Unix(int64(timeUnix), 0)
			if time.Now().After(expirationTime) {
				ExpireRecord(app, iter.Key(), PrefixKey(DB_DATA_PREFIX, iter.Value()))
				LogI(fmt.Sprintf("Record with key %s is expired and garbage collected.", string(iter.Value())))
			} else {
				break
			}
		}
		// Garbage collector cooldown
		time.Sleep(100 * time.Millisecond)
	}
}

// Processing queue with worker
func workerProcess(app *Application) {
	for {
		var request Request
		if app.requests.Lock(); app.requests.list.Len() > 0 {
			request = PullRequest(app)
			app.requests.Unlock()
		} else {
			app.requests.Unlock()
			// Doing nothing if no requests pending
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// In case record is going to be modified, lock it
		if request.command.argCommand == PUT || request.command.argCommand == DELETE {
			if !LockValue(app, string(request.command.argKey)) {
				request.conn.Write([]byte("Failed to modify record because it's locked\n"))
				request.conn.Close()
				continue
			}
		}

		// Comand switch
		switch request.command.argCommand {
		case PUT:
			expirationTime := strconv.Itoa(int(request.command.argExpirationTime.Unix()))
			// Creating batch for atomicity
			batch := new(leveldb.Batch)
			// In case already exists, updates expiration time
			if _, err := app.db.Get(PrefixKey(DB_DATA_PREFIX, request.command.argKey), nil); err == nil {
				batch.Delete(findExpiration(app, request.command.argKey))
			}
			batch.Put(PrefixKey(DB_DATA_PREFIX, request.command.argKey), request.command.argValue)
			experationBytes := []byte(expirationTime)
			// Creating checksum out of key for unique expiration record
			expChecksum := md5.Sum(request.command.argKey)
			batch.Put(PrefixKey(DB_EXPIRATION_PREFIX, append(experationBytes, expChecksum[:]...)), request.command.argKey)
			err := app.db.Write(batch, nil)
			UnlockValue(app, string(request.command.argKey))
			if err != nil {
				request.conn.Write([]byte("Error while performing PUT\n"))
				fmt.Println(err.Error())
				request.conn.Close()
				continue
			}
		case DELETE:
			// Batch for deleting both expiration and value record
			batch := new(leveldb.Batch)
			batch.Delete(PrefixKey(DB_DATA_PREFIX, request.command.argKey))
			batch.Delete(findExpiration(app, request.command.argKey))
			err := app.db.Write(batch, nil)
			UnlockValue(app, string(request.command.argKey))
			if err != nil {
				request.conn.Write([]byte("Error while performing DELETE\n"))
				request.conn.Close()
				continue
			}
		case READ:
			value, err := app.db.Get(PrefixKey(DB_DATA_PREFIX, request.command.argKey), nil)
			if err != nil {
				if err == leveldb.ErrNotFound {
					request.conn.Write([]byte("Value with such key cannot be found\n"))
				} else {
					request.conn.Write([]byte("Error getting record\n"))
				}
				request.conn.Close()
				continue
			}
			request.conn.Write([]byte(fmt.Sprintf("Value with key %s: %s\n", string(request.command.argKey), string(value))))
		}
		request.conn.Write([]byte(fmt.Sprintf("Succesfuly performed operation in %d ms\n", int64(time.Since(request.connectionTime)/time.Millisecond))))
		request.conn.Close()
	}
}

// Finding expiration date record by it's value for removing record before it's expired
func findExpiration(app *Application, value []byte) []byte {
	iter := app.db.NewIterator(util.BytesPrefix([]byte(DB_EXPIRATION_PREFIX)), nil)
	for iter.Next() {
		if bytes.Compare(iter.Value(), value) == 0 {
			return iter.Key()
		}
	}
	return []byte{}
}

func parseCommand(app *Application, request string) (Command, error) {
	var command Command
	// Parsing fields splitted with spaces
	switch fields := strings.Fields(request); strings.ToUpper(fields[0]) {
	case PUT:
		expirationTime, err := strconv.Atoi(fields[2])
		if err != nil {
			return command, errors.New("Wrong expiration time (should be one number in seconds)\n")
		}
		command.argCommand = PUT
		command.argKey = []byte(fields[1])
		command.argValue = []byte(strings.Join(fields[3:], " "))
		// Adding to expiration time user defined quantity of seconds
		command.argExpirationTime = time.Now().Add(time.Duration(expirationTime) * time.Second)
	case READ:
		command.argCommand = READ
		command.argKey = []byte(fields[1])
	case DELETE:
		command.argCommand = DELETE
		command.argKey = []byte(fields[1])
	default:
		command.argCommand = fields[0]
		command.argValue = []byte(fields[1])
		// In case none of general commands belong to request, trying to find it in server command slice
		if !app.CheckServerCommand(command) {
			return command, errors.New("Unknown command. Please, try READ, PUT, DELETE.\n")
		}
	}
	return command, nil
}

func HandleRequest(app *Application, conn net.Conn) {

	// Making buffer size + 1 to identify if byte limit is exceeded
	exceedingSize := app.maxSize + 1
	// Time capture for measuring request speed
	currentTime := time.Now()

	buff := make([]byte, exceedingSize)
	n, err := conn.Read(buff)

	if err != nil {
		if err != io.EOF {
			LogE("Failed to read data: " + err.Error())
			return
		}
	} else if n == exceedingSize {
		conn.Write([]byte("Request exceeds max possible size\n"))
		LogE("Request exceeds max possible size")
		return
	}

	// Request parsing into command
	requestCommand, err := parseCommand(app, string(buff[:n]))

	if err != nil {
		conn.Write([]byte(err.Error() + "\n"))
		conn.Close()
		return
	}

	request := Request{
		conn:           conn,
		connectionTime: currentTime,
		command:        requestCommand,
	}

	// Adding made up request into queue
	AddRequest(app, request)

	LogI(fmt.Sprintf("Request from %s pushed to queue", conn.RemoteAddr().String()))
}
