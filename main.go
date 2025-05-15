package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const DEFAULT_PORT = 6379

type role = string

const (
	MASTER role = "master"
	SLAVE  role = "slave"
)

type Value struct {
	value string
	exp   time.Time
}

type Server struct {
	host string
	port int
}

func (s *Server) String() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("%s %d", s.host, s.port)
}

func (s *Server) Set(str string) error {
	fields := strings.Fields(str)
	if len(fields) != 2 {
		return fmt.Errorf("expected --replicaof='<MASTER_HOST> <MASTER_PORT>'")
	}
	host := fields[0]
	port, err := strconv.Atoi(fields[1])
	if err != nil {
		return fmt.Errorf("invalid port '%s'", fields[1])
	}

	s.host = host
	s.port = port
	return nil
}

type Config struct {
	db               map[string]Value
	dir              string
	dbFileName       string
	port             int
	role             role
	replicaof        Server
	masterReplid     string
	masterReplOffset int
}

var cfg = Config{
	db:   make(map[string]Value),
	role: MASTER,
	masterReplOffset: 0,
}

func parseArgs() {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf(red("FAILER: %s"), err)
	}
	flag.StringVar(&cfg.dir, "dir", dir, "the path to the directory where the RDB file is stored")
	flag.StringVar(&cfg.dbFileName, "dbfilename", "dump.rdb", "the name of the RDB file")
	flag.IntVar(&cfg.port, "port", DEFAULT_PORT, "the port that the redis server will listen on")
	flag.Var(&cfg.replicaof, "replicaof", "connect to master server and run in replica mode")
	flag.Parse()

	if cfg.replicaof != (Server{}) {
		cfg.role = SLAVE
		return
	} 

	cfg.masterReplid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}

func listenAndServe() {
	l, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", cfg.port))
	if err != nil {
		log.Fatalf(red("FAILER: %s"), err)
	}

	log.Printf(green("Listening on port %d"), cfg.port)
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf(yellow("ERROR: %s"), err)
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		cmd, err := parseCommand(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf(yellow("ERROR: %s"), err)
			}
			return
		}

		log.Print(grey("=====RESPONSE====="))
		switch cmd.name {
		case "ping":
			handlePING(cmd, conn)
		case "echo":
			handleECHO(cmd, conn)
		case "set":
			handleSET(cmd, conn)
		case "get":
			handleGET(cmd, conn)
		case "config":
			handleCONFIG(cmd, conn)
		case "keys":
			handleKEYS(cmd, conn)
		case "info":
			handleINFO(cmd, conn)
		}
	}
}

func main() {
	parseArgs()

	if err := loadDataFromRDBFile(); err != nil {
		log.Fatalf(red("FAILER: %s"), err)
	}

	listenAndServe()
}
