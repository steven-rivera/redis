package main

import (
	"fmt"
	"log"
	"net"
)

var db = make(map[string]string)

func main() {
	l, err := net.Listen("tcp", "localhost:6379")
	if err != nil {
		log.Fatal("Failed to bind to port 6379")
	}
	log.Println("Listening on port 6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("Error accepting connection: ", err.Error())
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		cmd, err := parseCommand(conn)
		if err != nil {
			log.Print(err)
			return
		}

		switch cmd.name {
		case "ping":
			conn.Write([]byte("+PONG\r\n"))
		case "echo":
			for _, arg := range cmd.args {
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
				conn.Write([]byte(resp))
			}
		case "set":
			db[cmd.args[0]] = cmd.args[1]
			conn.Write([]byte("+OK\r\n"))
		case "get":
			val, ok := db[cmd.args[0]]
			if ok {
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
				conn.Write([]byte(resp))
			} else {
				conn.Write([]byte("$-1\r\n"))
			}
		}

	}
}
