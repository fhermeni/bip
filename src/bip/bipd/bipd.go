/**
 *
 *
 * @author Fabien Hermenier
 */
package main

import (
	"bip"
	"flag"
	"os"
	"log")

func main() {
	port := flag.Int("p", 6798, "Listening port")
	root := flag.String("r", "./bip_data", "Directory where data are stored")

	flag.Parse()

	idx, err := bip.NewIndex(*root)
	if err != nil {
		log.Fatalf("Unable to create the index: %s\n", err)
		os.Exit(1)
	}
	log.Printf("Index created in '%s' with %d jobs\n", *root, len(idx.ListJobs()))
	log.Printf("Listening on %d...\n", *port)
	err = bip.StartREST(*idx, *port)
	if (err != nil) {
		log.Fatalf("Unable to start the Rest service: %s\n", err)
		os.Exit(1)
	}
}
