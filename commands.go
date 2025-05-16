package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

func propagateCommand(cmd *Command, conn net.Conn) {
	writeCommands := map[string]bool{
		"set": true,
		"del": true,
	}

	if _, ok := writeCommands[cmd.name]; !ok {
		return
	}

	log.Printf(grey("Propagating command: %s"), cmd.name)

	conn.Write(fmt.Appendf(nil, "*%d\r\n$%d\r\n%s\r\n", 1+len(cmd.args), len(cmd.name), cmd.name))
	for _, arg := range cmd.args {
		conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(arg), arg))
	}
}

func handlePING(_ *Command, conn net.Conn) {
	log.Print(cyan("> +PONG"))
	conn.Write([]byte("+PONG\r\n"))
}

func handleECHO(cmd *Command, conn net.Conn) {
	for _, arg := range cmd.args {
		log.Printf(cyan("> $%d"), len(arg))
		log.Printf(cyan("> $%s"), arg)
		conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(arg), arg))
	}
}

func handleSET(cmd *Command, conn net.Conn) {
	key := cmd.args[0]
	value := Value{value: cmd.args[1]}

	if len(cmd.args) >= 4 {
		units := cmd.args[2]
		num, err := strconv.Atoi(cmd.args[3])
		if err != nil {
			if cfg.role == MASTER {
				conn.Write(fmt.Appendf(nil, "-ERR %s", err))
			}
			return
		}
		if strings.ToLower(units) == "px" {
			value.exp = time.Now().Add(time.Duration(num) * time.Millisecond)
		}
	}

	cfg.db[key] = value
	if cfg.role == MASTER {
		log.Print(cyan("> +OK"))
		conn.Write([]byte("+OK\r\n"))
	}

}

func handleGET(cmd *Command, conn net.Conn) {
	val, ok := cfg.db[cmd.args[0]]

	if ok && (time.Now().Before(val.exp) || val.exp.IsZero()) {
		log.Printf(cyan("> $%d"), len(val.value))
		log.Printf(cyan("> $%s"), val.value)
		conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val.value), val.value))
		return
	}

	conn.Write([]byte("$-1\r\n"))
}

func handleCONFIG(cmd *Command, conn net.Conn) {
	if strings.ToLower(cmd.args[0]) == "get" {
		switch strings.ToLower(cmd.args[1]) {
		case "dir":
			log.Print(cyan("> *2"))
			log.Print(cyan("> $3"))
			log.Print(cyan("> dir"))
			log.Printf(cyan("> $%d"), len(cfg.dir))
			log.Printf(cyan("> %s"), cfg.dir)
			conn.Write(fmt.Appendf(nil, "*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(cfg.dir), cfg.dir))
		case "dbfilename":
			log.Print(cyan("> *2"))
			log.Print(cyan("> $10"))
			log.Print(cyan("> dbfilename"))
			log.Printf(cyan("> $%d"), len(cfg.dbFileName))
			log.Printf(cyan("> %s"), cfg.dbFileName)
			conn.Write(fmt.Appendf(nil, "*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(cfg.dbFileName), cfg.dbFileName))
		}
	}
}

func handleKEYS(_ *Command, conn net.Conn) {
	log.Printf(cyan("> *%d"), len(cfg.db))
	conn.Write(fmt.Appendf(nil, "*%d\r\n", len(cfg.db)))
	for key := range cfg.db {
		log.Printf(cyan("> $%d"), len(key))
		log.Printf(cyan("> %s"), key)
		conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(key), key))
	}
}

func handleINFO(cmd *Command, conn net.Conn) {
	if cmd.args[0] == "replication" {
		pairs := []string{}
		pairs = append(pairs, fmt.Sprintf("role:%s", cfg.role))
		pairs = append(pairs, fmt.Sprintf("master_replid:%s", cfg.masterReplid))
		pairs = append(pairs, fmt.Sprintf("master_repl_offset:%d", cfg.masterReplOffset))

		resp := strings.Join(pairs, "\r\n")

		log.Printf(cyan("> $%d"), len(resp))
		for _, pair := range pairs {
			log.Printf(cyan("> %s"), pair)
		}

		conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(resp), resp))
	}
}

func handleREPLCONF(_ *Command, conn net.Conn) {
	log.Print(cyan("> +OK"))
	conn.Write([]byte("+OK\r\n"))
}

func handlePSYNC(_ *Command, conn net.Conn) {
	log.Print(cyan("> +FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"))
	conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))

	emptyRDBfile := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
	data, _ := base64.StdEncoding.DecodeString(emptyRDBfile)
	log.Printf(cyan("> $%d"), len(data))
	log.Printf(cyan("> %#x"), data)
	conn.Write(fmt.Appendf(nil, "$%d\r\n%s", len(data), data))

	cfg.slaves = append(cfg.slaves, conn)
}
