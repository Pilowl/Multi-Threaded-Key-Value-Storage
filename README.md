# Multi-Threaded Key-Value storage with expiration date
  Basic thread-safe key-value temporary storage on TCP sockets using LevelDB. Values and keys are divided using prefixes for more effective value storing and accessing in pairs [key -> value] and [experation date in Unix -> key]. Expiration date checking is implemented with iterating over keys with "experation date prefix". Because values are stored in lexographical order, there is no need to iterate over all experation dates. 
 Also there was an idea to store everything in one key-value pair while encoding expiration date in first 10 bytes, but in this case everything should be accessed using iterator which doesn't look as effective. In expiration date key also is md5 encoded key from value in case if some of the values will be put into database at the same time unit, to preserve key uniqueness and to avoid record leaks.
## Prerequisites

- GOlang
- GOlang levelDB implementation https://github.com/syndtr/goleveldb 

## Getting project dependencies

In project folder: `go get -u ./`

## Server

Use master file to start server or ```go run main.go utils.go server.go``` command where at the end flags can be added. Available flags can be found using `--help` command. Among them:
- `-port` for port changing
- `-dbpath` for specifying database path
- `-workers` for specifying request threads

Also max request (by default - 256 byte) size can be changed using tcp request: 
```PUT DB_REQ_SIZE *some integer*```

## Commands
- `READ *KEY*` - for getting value from the storage
- `PUT *KEY* *STORING TIME IN SECONDS* *VALUE*` - for inserting value  
- `DELETE *KEY*` - for deleting value

Also can be tested using netcat:
- `echo -n "READ somekey" | nc localhost 2222`
- `echo -n "PUT somekey 100 somevalue" | nc localhost 2222`
- `echo -n "DELETE somekey" | nc localhost 2222`

There are also some dumb load and expiration date tests can be found in the test folder.
