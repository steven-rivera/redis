package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type Value struct {
	value string
	exp   time.Time
}

var db = make(map[string]Value)

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
			handleSET(cmd, conn)
		case "get":
			handleGET(cmd, conn)
		}

	}
}

func handleSET(cmd *Command, conn net.Conn) {
	key := cmd.args[0]
	value := Value{
		value: cmd.args[1],
	}
	if len(cmd.args) > 2 {
		units := cmd.args[2]
		num, err := strconv.Atoi(cmd.args[3])
		if err != nil {
			log.Print(err)
			return
		}
		if strings.ToLower(units) == "px" {
			value.exp = time.Now().Add(time.Duration(num) * time.Millisecond)
		}
	}
	log.Printf("%s -> %+v", key, value)
	db[key] = value
	conn.Write([]byte("+OK\r\n"))

}

func handleGET(cmd *Command, conn net.Conn) {
	val, ok := db[cmd.args[0]]
	
	if ok && (time.Now().Before(val.exp) || val.exp.IsZero()) {
		resp := fmt.Sprintf("$%d\r\n%s\r\n", len(val.value), val.value)
		conn.Write([]byte(resp))
		return
	}

	conn.Write([]byte("$-1\r\n"))
}
