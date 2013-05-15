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
var commands map[string]func([]string)

func ListJobs(args []string) {
	res, err := http.Get(remote + "/jobs/")
	if (err != nil) {
		fmt.Fprintf(os.Stderr, "Unable to list the jobs: %s\n", err)
		os.Exit(1)
	}
	if (res.StatusCode == http.StatusOK) {
		cnt, _ := ioutil.ReadAll(res.Body)
		fmt.Printf("%s", cnt)
	} else {
		errorMsgAndQuit(res, 2)
	}
}

func Push(args [] string) {
	id := args[0]
	res, err := http.Post(remote + "/jobs/?j=" + id, "", os.Stdin)
	if (err != nil) {
		fmt.Fprintf(os.Stderr, "Unable to submit the job: %s\n", err)
		os.Exit(-1)
	}
	if (res.StatusCode == http.StatusConflict) {
		fmt.Fprintf(os.Stderr, "Job '%s' already exists\n", id)
		os.Exit(2)
	} else if (res.StatusCode != http.StatusCreated) {
		errorMsgAndQuit(res, 2)
	}
}

func Process(args []string) {
	if (len(args) == 0) {
		fmt.Println("Process a random job")
		//Get a random processable job
		req, err := http.NewRequest("PUT", remote + "/jobs/", nil)
		if (err != nil) {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(-1)
		}
		res, err := http.DefaultClient.Do(req)
		if (err != nil) {
			fmt.Fprintf(os.Stderr, "Unable to submit the request: %s\n", err)
			os.Exit(-1)
		} else if (res.StatusCode == http.StatusOK) {
			cnt, _ := ioutil.ReadAll(res.Body)
			fmt.Printf("%s", cnt)
		} else if (res.StatusCode == http.StatusNoContent) {
			os.Exit(3)
		} else {
			errorMsgAndQuit(res, 2)
		}
	} else if (len(args) == 1) {
		//process a given job
		fmt.Printf("Process job %s\n", args[0])
		req, err := http.NewRequest("PUT", remote + "/jobs/" + args[0] + "/status?s=processing", nil)
		if (err != nil) {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(-1)
		}
		res, err := http.DefaultClient.Do(req)
		if (err != nil) {
			fmt.Fprintf(os.Stderr, "Unable to submit the request: %s\n", err)
			os.Exit(-1)
		}
		if (res.StatusCode != http.StatusOK) {
			errorMsgAndQuit(res, 2)
		}
	}

}

func errorMsgAndQuit(res *http.Response, exitCode int) {
	cnt, _ := ioutil.ReadAll(res.Body)
	fmt.Fprintf(os.Stderr, "Error '%s': %s", res.Status, cnt)
	os.Exit(exitCode)
}

func Commit(args []string) {
	id := args[0]
	req, err := http.NewRequest("PUT", remote + "/jobs/" + id + "/status?s=terminated", nil)
	if (err != nil) {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(-1)
	}
	res, err := http.DefaultClient.Do(req)
	if (err != nil) {
		fmt.Fprintf(os.Stderr, "Unable to submit the request: %s\n", err)
		os.Exit(-1)
	}
	if (res.StatusCode != http.StatusOK) {
		errorMsgAndQuit(res, 2)
	}
}

func Done(args []string) {
	id := args[0]
	req, err := http.NewRequest("PUT", remote + "/jobs/" + id + "/status?s=terminating", nil)
	if (err != nil) {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(-1)
	}
	res, err := http.DefaultClient.Do(req)
	if (err != nil) {
		fmt.Fprintf(os.Stderr, "Unable to submit the request: %s\n", err)
		os.Exit(-1)
	}
	if (res.StatusCode != http.StatusOK) {
		errorMsgAndQuit(res, 2)
	}
}


func PutResult(args []string) {
	id := args[0]
	r := args[1]
	res, err := http.Post(remote + "/jobs/" + id + "/results/?r=" + r, "", os.Stdin)
	if (err != nil) {
		fmt.Fprintf(os.Stderr, "Unable to submit the request: %s\n", err)
		os.Exit(-1)
	}
	if (res.StatusCode != http.StatusCreated) {
		errorMsgAndQuit(res, 2)
	}
}

func get(url string) {
	res, err := http.Get(remote + url)
	if (err != nil) {
		fmt.Fprintf(os.Stderr, "Error while sending the request: %s\n", err)
		os.Exit(-1)
	}
	if (res.StatusCode == http.StatusOK) {
		cnt, _ := ioutil.ReadAll(res.Body)
		fmt.Printf("%s", cnt)
	} else {
		errorMsgAndQuit(res, 2)
	}
}

func Data(args []string) {
	get("/jobs/" + args[0] + "/data")
}

func Status(args []string) {
	get("/jobs/" + args[0] + "/status")
}

func GetJob(args []string) {
	get("/jobs/" + args[0])
}

func Result(args []string) {
	get("/jobs/" + args[0] + "/results/" + args[1])
}

func Results(args []string) {
	get("/jobs/" + args[0] + "/results/")
}

func main() {

	commands = make(map[string]func([]string))
	commands["list"] = ListJobs
	commands["put"] = Push
	commands["process"] = Process
	commands["done"] = Done
	commands["rput"] = PutResult
	commands["commit"] = Commit
	commands["get"] = GetJob
	commands["status"] = Status
	commands["data"] = Data
	commands["rlist"] = Results
	commands["rget"] = Result

	if len(os.Args) < 2 {
		Usage(os.Args)
		os.Exit(1)
	}

	if (os.Args[1] == "help") {
		Usage(os.Args)
		os.Exit(0)
	} else if (len(os.Args) == 2) {
		Usage(os.Args)
		os.Exit(1)
	}
	remote = "http://" + os.Args[1]
	fn, ok := commands[os.Args[2]]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown command '%s'. 'bip help' for help\n", os.Args[2])
		os.Exit(1)
	}
	fn(os.Args[3:])
}

func Usage(args []string) {
	fmt.Fprintf(os.Stderr, "Usage: 'bip server command'\n")
	fmt.Fprintf(os.Stderr, "server: the server to connect to 'host:port' format\n")
	fmt.Fprintf(os.Stderr, "Available commands:\n")
	fmt.Fprintf(os.Stderr, " list - list the jobs\n")
	fmt.Fprintf(os.Stderr, " put - declare a new job\n")
	fmt.Fprintf(os.Stderr, " process - process a job\n")
	fmt.Fprintf(os.Stderr, " done - declare a job processing is done\n")
	fmt.Fprintf(os.Stderr, " rput - send a result\n")
	fmt.Fprintf(os.Stderr, " commit - declare a job has been processed and all the results sended\n")
	fmt.Fprintf(os.Stderr, " get - get a job summary\n")
	fmt.Fprintf(os.Stderr, " status - get a job status\n")
	fmt.Fprintf(os.Stderr, " data - get a job data\n")
	fmt.Fprintf(os.Stderr, " rlist - get the results identifier of a processed job\n")
	fmt.Fprintf(os.Stderr, " rget - get a specific results for a processed job\n")
	fmt.Fprintf(os.Stderr, " help - print this help\n")
	fmt.Fprintf(os.Stderr, "\n'bip help command' for a complete description\n")
}
