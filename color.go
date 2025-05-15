package main

import "fmt"

const (
	RED     = "\x1b[31m"
	GREEN   = "\x1b[32m"
	YELLOW  = "\x1b[33m"
	BLUE    = "\x1b[34m"
	MAGENTA = "\x1b[35m"
	CYAN    = "\x1b[36m"
	GREY    = "\x1b[90m"
	RESET   = "\x1b[0m"
)

func red(text string) string {
	return fmt.Sprintf("%s%s%s", RED, text, RESET)
}

func green(text string) string {
	return fmt.Sprintf("%s%s%s", GREEN, text, RESET)
}

func yellow(text string) string {
	return fmt.Sprintf("%s%s%s", YELLOW, text, RESET)
}

func grey(text string) string {
	return fmt.Sprintf("%s%s%s", GREY, text, RESET)
}

func blue(text string) string {
	return fmt.Sprintf("%s%s%s", BLUE, text, RESET)
}

func magenta(text string) string {
	return fmt.Sprintf("%s%s%s", MAGENTA, text, RESET)
}

func cyan(text string) string {
	return fmt.Sprintf("%s%s%s", CYAN, text, RESET)
}
