package main

import (
	"fmt"
	"os"
	"sort"
)

type command struct {
	description string
	fn          func(args []string)
}

// All subcommands should add themselves to this list in init().
var commandsByName = make(map[string]command)

func usage() {
	var commandNames []string
	for name := range commandsByName {
		commandNames = append(commandNames, name)
	}
	sort.Strings(commandNames)

	fmt.Println("gumtool is part of gumshoedb. It has several subcommands:")
	for _, name := range commandNames {
		cmd := commandsByName[name]
		fmt.Printf("  %-8s %s\n", name, cmd.description)
	}

	fmt.Printf(`Usage:
  $ %s COMMAND [ARGS]
Run '%[1]s COMMAND -h to see how to use a particular command
`, os.Args[0])
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	commandName := os.Args[1]
	switch commandName {
	case "help", "-h", "--help":
		usage()
	}
	cmd, ok := commandsByName[commandName]
	if !ok {
		fmt.Printf("Error: no such command %q\n\n", commandName)
		usage()
	}
	cmd.fn(os.Args[2:])
}

func fatalln(args ...interface{}) {
	fmt.Println(args...)
	os.Exit(1)
}

func fatalf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	os.Exit(1)
}
