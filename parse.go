package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

type commandState = int

const (
	commandStateParsingCmd = iota
	commandStateParsingArgs
	commandStateDone
)

type Command struct {
	name  string
	args  []string
	state commandState
}

func parseCommand(client *bufio.Scanner, remote net.Addr) (*Command, int, error) {
	bytesProc := 0
	cmd := &Command{
		state: commandStateParsingCmd,
	}

	if ok := client.Scan(); !ok {
		err := client.Err()
		if err != nil {
			return nil, bytesProc, err
		}
		return nil, bytesProc, io.EOF
	}

	lines := []string{}

	text := client.Text()
	bytesProc += (len(text) + 2)
	lines = append(lines, text)
	numParams, err := strconv.Atoi(strings.TrimPrefix(text, "*"))
	if err != nil {
		return nil, 0, err
	}

	for range numParams {
		client.Scan() // Skip line containing length Ex: $4\r\n
		text := client.Text()
		bytesProc += (len(text) + 2)
		lines = append(lines, text)

		client.Scan()
		text = client.Text()
		bytesProc += (len(text) + 2)
		lines = append(lines, text)

		switch cmd.state {
		case commandStateParsingCmd:
			cmd.name = strings.ToLower(text)
			cmd.state = commandStateParsingArgs
		case commandStateParsingArgs:
			cmd.args = append(cmd.args, text)
		}
	}
	log.Printf(magenta("=== REQUEST %+v ===\n")+magenta("> %s"), remote, strings.Join(lines, " "))
	cmd.state = commandStateDone
	return cmd, bytesProc, nil
}
