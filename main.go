package main

import (
	"flag"
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

type Config struct {
	db         map[string]Value
	dir        string
	dbFileName string
}

var cfg = Config{
	db:         make(map[string]Value),
}

func main() {
	flag.StringVar(&cfg.dir, "dir", "/tmp/redis-files", "the path to the directory where the RDB file is stored")
	flag.StringVar(&cfg.dbFileName, "dbfilename", "rdbfile", "the name of the RDB file")
	flag.Parse()

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
		case "config":
			handleCONFIG(cmd, conn)
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
	cfg.db[key] = value
	conn.Write([]byte("+OK\r\n"))

}

func handleGET(cmd *Command, conn net.Conn) {
	val, ok := cfg.db[cmd.args[0]]

	if ok && (time.Now().Before(val.exp) || val.exp.IsZero()) {
		resp := fmt.Sprintf("$%d\r\n%s\r\n", len(val.value), val.value)
		conn.Write([]byte(resp))
		return
	}

	conn.Write([]byte("$-1\r\n"))
}

func handleCONFIG(cmd *Command, conn net.Conn) {
	if strings.ToLower(cmd.args[0]) == "get" {
		switch strings.ToLower(cmd.args[1]) {
		case "dir":
			resp := fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(cfg.dir), cfg.dir)
			conn.Write([]byte(resp))
		case "dbfilename":
			resp := fmt.Sprintf("*2\r\n$3\r\ndbfilename\r\n$%d\r\n%s\r\n", len(cfg.dbFileName), cfg.dbFileName)
			conn.Write([]byte(resp))
		}
	}
}
