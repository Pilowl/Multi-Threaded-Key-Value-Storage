package main

// Function for adding prefix to byte slice that is key
func PrefixKey(prefix string, key []byte) []byte {
	return append([]byte(prefix), key...)
}
