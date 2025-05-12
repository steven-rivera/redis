package main

import (
	"fmt"
	"log"
	"net"
)

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
		}
	}
}
