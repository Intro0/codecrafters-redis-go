package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Entry struct {
		value string
		expiry time.Time
	}

func main() {

	// storage
	storage := make(map[string]Entry)

	// listener
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	// main loop
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		// handle multiple clients thru concurrency
		go handleConnection(conn,storage)
	}
}

// handles one client
func handleConnection(conn net.Conn,storage map[string]Entry) {
	for {
		buf:=make([]byte, 1024)
		n,err := conn.Read(buf)
		if err != nil {
			// check if client left so we dont need to print error
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading from connection: ", err.Error())
			break
		}
		// parses arguments from input
		parts := strings.Split(string(buf[:n]),"\r\n")
		switch strings.ToLower(parts[2]) {
			case "ping":
				conn.Write([]byte("+PONG\r\n"))
			case "echo":
				input := parts[4]
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(input), input)
				conn.Write([]byte(response))
			case "set":
				expiry := time.Time{}
				if len(parts) > 9 {
					switch strings.ToUpper(parts[8]) {
						case "PX":
							ms,err := strconv.Atoi(parts[10])
							if err != nil {
								fmt.Println("Error with PX: ", err.Error())
							}
							expiry = time.Now().Add(time.Duration(ms) * time.Millisecond)
						case "EX":
							s,err := strconv.Atoi(parts[10])
							if err != nil {
								fmt.Println("Error with PX: ", err.Error())
							}
							expiry = time.Now().Add(time.Duration(s) * time.Second)
						default:
							fmt.Println("invalid syntax")
					}
				}
				key := parts[4]
				value := parts[6]
				storage[key] = Entry{value:value,expiry:expiry}
				conn.Write([]byte("+OK\r\n"))
			case "get":
				key := parts[4]
				input,ok := storage[key]
				if !ok {
					fmt.Println("value not found")
					conn.Write([]byte("$-1\r\n"))
					continue
				}
				if !input.expiry.IsZero() && time.Now().After(input.expiry) {
					fmt.Println("value expired")
					conn.Write([]byte("$-1\r\n"))
					continue
				}
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(input.value), input.value)
				conn.Write([]byte(response))
			default:
				fmt.Println("Unknown Syntax")
		}
	}
}
