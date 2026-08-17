package main

import (
	"fmt"
	"io"
	"os"
)

const courierVersion = "0.2.0"

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "courier:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printUsage(stderr)
		return fmt.Errorf("a subcommand is required")
	}

	switch args[0] {
	case "serve":
		return runServe(args[1:], stderr)
	case "mcp":
		return runMCP(args[1:], stderr)
	case "version":
		if len(args) != 1 {
			printUsage(stderr)
			return fmt.Errorf("version takes no arguments")
		}
		fmt.Fprintln(stdout, courierVersion)
		return nil
	default:
		printUsage(stderr)
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: courier <serve|mcp|version>")
	fmt.Fprintln(w, "       courier import  (reserved)")
}

func runServe(args []string, stderr io.Writer) error {
	if len(args) != 0 {
		printUsage(stderr)
		return fmt.Errorf("serve takes no arguments")
	}
	opts, err := LoadServeOptions(func(message string) {
		fmt.Fprintln(stderr, "[courier]", message)
	})
	if err != nil {
		return err
	}
	return serveMain(opts)
}

func runMCP(args []string, stderr io.Writer) error {
	if len(args) != 0 {
		printUsage(stderr)
		return fmt.Errorf("mcp takes no arguments")
	}
	opts, err := LoadMCPOptions()
	if err != nil {
		return err
	}
	return mcpMain(opts)
}
