/**
 *
 * CLI to get jobs.
 * @author Fabien Hermenier
 */
package main

import (
	"fmt"
	"os"
	"net/http"
	"io/ioutil"
)
var remote string

func usage() {
	fmt.Printf("Usage: location (list|push|pop|get-data|get...)\n")
}

func listJobs() {
	res, err := http.Get("http://" + remote + "/jobs/")
	if (err != nil) {
		fmt.Printf("Unable to list the jobs: %s\n", err)
		os.Exit(1)
	}
	if (res.StatusCode == http.StatusOK) {
		cnt, _ := ioutil.ReadAll(res.Body)
		fmt.Printf("%s", cnt)
	} else {
		fmt.Printf("Unable to list the jobs: %s\n", res.Status)
		os.Exit(2)
	}
}

func info(id string) {
	res, err := http.Get("http://" + remote + "/jobs/" + id)
	if (err != nil) {
		fmt.Printf("Unable to get job '%s': %s\n", id, err)
		os.Exit(-1)
	}
	if (res.StatusCode == http.StatusOK) {
		cnt, _ := ioutil.ReadAll(res.Body)
		fmt.Printf("%s", cnt)
	} else {
		fmt.Fprintf(os.Stderr, "Unable to get job '%s': %s\n", res.Status)
		os.Exit(2)
	}
}

func data(id string) {
	res, err := http.Get("http://" + remote + "/jobs/" + id + "/data")
	if (err != nil) {
		fmt.Printf("Unable to get job '%s': %s\n", id, err)
		os.Exit(1)
	}
	if (res.StatusCode == http.StatusOK) {
		cnt, _ := ioutil.ReadAll(res.Body)
		fmt.Printf("%s", cnt)
	} else {
		fmt.Printf("Unable to get job '%s': %s\n", id, res.Status)
		os.Exit(2)
	}
}


func push(id string) {
	res, err := http.Post("http://" + remote + "/jobs/" + id, "", os.Stdin)
	if (err != nil) {
		fmt.Printf("Unable to submit the job: %s\n", err)
		os.Exit(-1)
	}
	if (res.StatusCode == http.StatusConflict) {
		fmt.Printf("Job '%s' already exists\n", id)
		os.Exit(2)
	} else if (res.StatusCode != http.StatusCreated) {
		cnt, _ := ioutil.ReadAll(res.Body)
		fmt.Printf("Error while pushing job '%s': %s\n%s\n", id, res.Status, cnt)
		os.Exit(2)
	}
}

func pop() {
	fmt.Fprintf(os.Stderr, "Not implemented\n")
	os.Exit(1)
}

func commit(id string) {
	fmt.Fprintf(os.Stderr, "Not implemented\n")
	os.Exit(1)
}

func addResult(id string, r string) {
	fmt.Fprintf(os.Stderr, "Not implemented\n")
	os.Exit(1)
}

func main() {

	if len(os.Args) < 3 {
		usage()
		os.Exit(1)
	}

	remote = os.Args[1]
	switch(os.Args[2]) {
		case "list": listJobs()
		case "info": info(os.Args[3])
		case "get-data" : data(os.Args[3])
		case "push" : push(os.Args[3])
		case "pop" : pop()
		case "add-result" : addResult(os.Args[3], os.Args[4])
		case "commit" : commit(os.Args[3])
	default:
		fmt.Printf("Unsupported operation '%s'\n", os.Args[2])
		os.Exit(1)
	}
}

