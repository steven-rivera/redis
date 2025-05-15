package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

const DEFAULT_PORT = 6379

type Value struct {
	value string
	exp   time.Time
}

type Config struct {
	db         map[string]Value
	dir        string
	dbFileName string
	port       uint
}

var cfg = Config{db: make(map[string]Value)}

func parseArgs() {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf(red("FAILER: %s"), err)
	}
	flag.StringVar(&cfg.dir, "dir", dir, "the path to the directory where the RDB file is stored")
	flag.StringVar(&cfg.dbFileName, "dbfilename", "dump.rdb", "the name of the RDB file")
	flag.UintVar(&cfg.port, "port", DEFAULT_PORT, "the port that the redis server will listen on")
	flag.Parse()
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
		}
	}
}

func main() {
	parseArgs()
	err := loadDataFromRDBFile()
	if err != nil {
		log.Fatalf(red("FAILER: %s"), err)
	}
	listenAndServe()
}
