package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

const DEFAULT_PORT = "6379"

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
	port string
}

func (s *Server) String() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("%s %s", s.host, s.port)
}

func (s *Server) Set(str string) error {
	fields := strings.Fields(str)
	if len(fields) != 2 {
		return fmt.Errorf("expected --replicaof='<MASTER_HOST> <MASTER_PORT>'")
	}

	s.host = fields[0]
	s.port = fields[1]
	return nil
}

type Config struct {
	db               map[string]Value
	dir              string
	dbFileName       string
	port             string
	role             role
	replicaof        Server
	masterReplid     string
	masterReplOffset int
	slaves           []net.Conn
}

var cfg = Config{
	db:               make(map[string]Value),
	role:             MASTER,
	masterReplOffset: 0,
}

func parseArgs() {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf(red("FAILER: %s"), err)
	}
	flag.StringVar(&cfg.dir, "dir", dir, "the path to the directory where the RDB file is stored")
	flag.StringVar(&cfg.dbFileName, "dbfilename", "dump.rdb", "the name of the RDB file")
	flag.StringVar(&cfg.port, "port", DEFAULT_PORT, "the port that the redis server will listen on")
	flag.Var(&cfg.replicaof, "replicaof", "connect to master server and run in replica mode")
	flag.Parse()

	if cfg.replicaof != (Server{}) {
		cfg.role = SLAVE
		return
	}

	cfg.masterReplid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}

func listenAndServe() {
	l, err := net.Listen("tcp", net.JoinHostPort("localhost", cfg.port))
	if err != nil {
		log.Fatalf(red("FAILER: %s"), err)
	}

	log.Printf(green("Listening on port %s"), cfg.port)
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

	client := bufio.NewScanner(conn)
	for {
		cmd, err := parseCommand(client)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf(yellow("ERROR: %s"), err)
			}
			return
		}
		
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
		case "replconf":
			handleREPLCONF(cmd, conn)
		case "psync":
			handlePSYNC(cmd, conn)
		}

		if cfg.role == MASTER {
			for _, slave := range cfg.slaves {
				propagateCommand(cmd, slave)
			}
		}
	}
}

func connectToMaster() {
	conn, err := net.Dial("tcp", net.JoinHostPort(cfg.replicaof.host, cfg.replicaof.port))
	if err != nil {
		log.Fatalf(red("FAILED to connect to master: %s"), err)
	}

	// HANDSHAKE
	c := bufio.NewReader(conn)

	// PING (1/3)
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	resp, _, _ := c.ReadLine()
	log.Printf(grey("GOT: %s"), resp)

	// REPLCONF (2/3)
	conn.Write(fmt.Appendf(nil, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(cfg.port), cfg.port))
	resp, _, _ = c.ReadLine()
	log.Printf(grey("GOT: %s"), resp)

	conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	resp, _, _ = c.ReadLine()
	log.Printf(grey("GOT: %s"), resp)

	// PSYNC (3/3)
	conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	resp, _, _ = c.ReadLine()
	log.Printf(grey("GOT: %s"), resp)

	if err := loadDataFromConn(c); err != nil {
		log.Fatalf(red("FATAL: %s"), err)
	}

	go handleConnection(conn)
}

func main() {
	parseArgs()

	if err := loadDataFromRDBFile(); err != nil {
		log.Fatalf(red("FAILER: %s"), err)
	}

	if cfg.role == SLAVE {
		connectToMaster()
	}
	listenAndServe()
}
