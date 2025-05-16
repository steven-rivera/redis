package main

import (
	"bufio"
	"io"
	"log"
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

func parseCommand(client *bufio.Scanner) (*Command, error) {
	cmd := &Command{
		state: commandStateParsingCmd,
	}

	if ok := client.Scan(); !ok {
		err := client.Err()
		if err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	log.Print(grey("=====REQUEST====="))

	text := client.Text()
	log.Printf(magenta("> %s"), text)
	numParams, err := strconv.Atoi(strings.TrimPrefix(text, "*"))
	if err != nil {
		return nil, err
	}

	for range numParams {
		client.Scan() // Skip line containing length Ex: $4\r\n
		text := client.Text()
		log.Printf(magenta("> %s"), text)

		client.Scan()
		text = client.Text()
		log.Printf(magenta("> %s"), text)

		switch cmd.state {
		case commandStateParsingCmd:
			cmd.name = strings.ToLower(text)
			cmd.state = commandStateParsingArgs
		case commandStateParsingArgs:
			cmd.args = append(cmd.args, text)
		}
	}
	cmd.state = commandStateDone
	return cmd, nil
}
