/**
 *
 * CLI to get jobs.
 * @author Fabien Hermenier
 */
package main

import (
	"fmt"
	"os"
)

func usage() {
	fmt.Printf("Usage: (-s|-d) location (list|add|del|start|commit|get...)\n")
}


func main() {

	local := true

	if len(os.Args) == 0 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[0]
	switch cmd {
		case "-s": local = false
		case "-d": local = true
		case "-h": usage()
		default:
			fmt.Printf("Unknown command '%s'.\n", cmd)
			os.Exit(1)
	}

	to := os.Args[1]
	fmt.Printf("to: %s, local: %b\n", to, local)
}

