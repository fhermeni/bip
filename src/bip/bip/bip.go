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
	"encoding/json"
	"flag"
)
var remote string
var commands map[string]Command

type Command struct {
	Id string
	ShortHelp string
	LongHelp string
	Fn func([]string)
}

func ListJobs(args []string) {
	flagSet := flag.NewFlagSet("", 0)
	toJSON := flagSet.Bool("to-json", false, "")
	withStatus :=  flagSet.Bool("with-status", false, "")
	flagSet.Parse(args)

	res, err := http.Get(remote + "/jobs/")
	if (err != nil) {
		fmt.Fprintf(os.Stderr, "Unable to list the jobs: %s\n", err)
		os.Exit(1)
	}
	if (res.StatusCode == http.StatusOK) {
		cnt, _ := ioutil.ReadAll(res.Body)
		if (!*toJSON) {
			var js interface {}
			json.Unmarshal(cnt, &js)
			m := js.([]interface{})
			for _, v := range m {
				job := v.(map[string]interface {})
				if !*withStatus {
					fmt.Printf("%s\n", job["id"])
				} else {
					fmt.Printf("%s\t%s\n", job["id"], job["status"])
				}
			}
		} else {
			fmt.Printf("%s", cnt)
		}
	} else {
		errorMsgAndQuit(res, 2)
	}
}

func Put(args [] string) {
	checkArity(args, 1, commands["put"])
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
	checkArity(args, 1, commands["commit"])
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

func checkArity(args [] string, nb int, c Command) {
	if len(args) != nb {
		fmt.Fprintf(os.Stderr, "Missing parameter(s). 'bip help %s' to help\n", c.Id)
		os.Exit(1)
	}
}

func Done(args []string) {
	checkArity(args, 1, commands["done"])
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
	checkArity(args, 2, commands["rput"])
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

func get(url string) []byte {
	res, err := http.Get(remote + url)
	if (err != nil) {
		fmt.Fprintf(os.Stderr, "Error while sending the request: %s\n", err)
		os.Exit(-1)
	}
	if (res.StatusCode == http.StatusOK) {
		cnt, _ := ioutil.ReadAll(res.Body)
		return cnt
	} else {
		errorMsgAndQuit(res, 2)
	}
	return nil
}

func Data(args []string) {
	checkArity(args, 1, commands["data"])
	fmt.Printf("%s",get("/jobs/" + args[0] + "/data"))
}

func Status(args []string) {
	checkArity(args, 1, commands["status"])
	fmt.Printf("%s",get("/jobs/" + args[0] + "/status"))
}

func GetJob(args []string) {
	flagSet := flag.NewFlagSet("", 0)
	toJSON := flagSet.Bool("to-json", false, "")
	withStatus :=  flagSet.Bool("with-status", false, "")
	flagSet.Parse(args)
	checkArity(flagSet.Args(), 1, commands["get"])
	cnt := get("/jobs/" + flagSet.Args()[0])
	if !*toJSON {
		var js interface {}
		json.Unmarshal(cnt, &js)
		job := js.(map[string]interface {})
			if !*withStatus {
				fmt.Printf("%s\n", job["id"])
			} else {
				fmt.Printf("%s\t%s\n", job["id"], job["status"])
			}
	} else {
		fmt.Printf("%s", cnt)
	}
}

func Result(args []string) {
	checkArity(args, 2, commands["rget"])
	fmt.Printf("%s", get("/jobs/" + args[0] + "/results/" + args[1]))
}

func Results(args []string) {
	checkArity(args, 1, commands["rlist"])
	fmt.Printf("%s",get("/jobs/" + args[0] + "/results/"))
}

func main() {

	flag.StringVar(&remote, "s", "localhost:6798", "The server to correspond with")
	flag.Parse()
	commands = make(map[string]Command)
	commands["list"] = Command{"list", "List the jobs",
							   "bip [-s server ] list [options]\nAvailable options:\n --to-json: for a json output\n --with-status: to print the jobs status too",
								ListJobs}
	commands["put"] = Command{"put", "Declare the job", "bip [-s server ] put id\n id: the job identifier\n The job data are provided from stdin", Put}
	commands["process"] = Command{"process", "Process a job", "bip [-s server ] process [id]\n id : the job identifier to process. If omitted, a random processable job is choosed", Process}
	commands["done"] = Command{"done", "Declare a job processing is done", "", Done}
	commands["rput"] = Command{"rput", "Send a result","", PutResult}
	commands["commit"] = Command{"commit", "Declare a job has been processed and all the results sended", "", Commit}
	commands["get"] = Command{"get", "Get a job summary", " --to-json: for a json output\n --with-status: to print the jobs status too", GetJob}
	commands["status"] = Command{"status", "Get a job status", "", Status}
	commands["data"] = Command{"data", "Get a job data", "",Data}
	commands["rlist"] = Command{"rlist", "Get the results identifier of a processed job", " --to-json: for a json output", Results}
	commands["rget"] = Command{"rget", "Get a specific results for a processed job", "",Result}
	commands["help"] = Command{"help", "Print this help or the usage of a specific command", "",Usage}

	if (len(flag.Args()) == 0) {
		Usage(flag.Args())
		os.Exit(1)
	}
	remote = "http://" + remote
	cmd, ok := commands[flag.Args()[0]]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown command '%s'. 'bip help' for help\n", os.Args[2])
		os.Exit(1)
	}

	cmd.Fn(flag.Args()[1:])
}

func Usage(args []string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: 'bip [-s server] command'\n")
		fmt.Fprintf(os.Stderr, "server: the server and port to correspond with.\n")
		fmt.Fprintf(os.Stderr, "Available commands:\n")
		for k, cmd := range commands {
			fmt.Fprintf(os.Stderr, " %s - %s\n", k, cmd.ShortHelp)
		}
	} else {
		cmd, ok := commands[args[0]]
		if !ok {
			fmt.Fprintf(os.Stderr, "Unknown command '%s'. 'bip help' for help\n", os.Args[2])
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "%s\nUsage: %s\n", cmd.ShortHelp, cmd.LongHelp)
	}
}
