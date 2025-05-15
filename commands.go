package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

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
			conn.Write(fmt.Appendf(nil, "-ERR %s", err))
			return
		}
		if strings.ToLower(units) == "px" {
			value.exp = time.Now().Add(time.Duration(num) * time.Millisecond)
		}
	}

	cfg.db[key] = value
	log.Print(cyan("> +OK"))
	conn.Write([]byte("+OK\r\n"))
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
