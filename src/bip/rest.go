/**
 * Rest API for Index.
 *
 * @author Fabien Hermenier
 */
package bip

import (
	"github.com/gorilla/mux"
	"net/http"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

func StartREST(i Index, port int) error {
	idx = i
	r := mux.NewRouter()
	r.HandleFunc("/jobs/", GetJobs).Methods("GET")
	r.HandleFunc("/jobs/", PopJob).Methods("PUT")
	r.HandleFunc("/jobs/{j}", PushJob).Methods("POST")
	r.HandleFunc("/jobs/{j}", makeJobHandler(GetJob)).Methods("GET")
	r.HandleFunc("/jobs/{j}/data", makeJobHandler(GetData)).Methods("GET")
	r.HandleFunc("/jobs/{j}/status", makeJobHandler(GetStatus)).Methods("GET")
	r.HandleFunc("/jobs/{j}/status", makeJobHandler(UpdateStatus)).Methods("PUT")
	r.HandleFunc("/jobs/{j}/results/", makeJobHandler(GetResults)).Methods("GET")
	r.HandleFunc("/jobs/{j}/results/{r}", makeJobHandler(GetResult)).Methods("GET")
	r.HandleFunc("/jobs/{j}/results/{r}", makeJobHandler(PutResult)).Methods("PUT")
	http.Handle("/", r)
	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

var idx Index

func logInternalError(w http.ResponseWriter, userMsg , serverMsg string) {
	http.Error(w, userMsg, http.StatusInternalServerError)
	log.Println(serverMsg)
}

func makeJobHandler(fn func(http.ResponseWriter, *http.Request, *Job)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["j"]
		j, ok := idx.GetJob(id)
		if !ok {
			http.Error(w, fmt.Sprintf("Job '%s' not found", id), http.StatusNotFound)
			return
		}
		fn(w, r, j)
	}
}

func UpdateStatus(w http.ResponseWriter, r * http.Request, j *Job) {
	//Get the status
	st, err := ioutil.ReadAll(r.Body)
	status := string(st)
	switch (status) {
		case "terminating": err = j.Terminating()
		case "terminated": err = j.Terminated()
		default:
			http.Error(w, fmt.Sprintf("non-viable status code: %s", status), http.StatusBadRequest)
			return
	}
	if (err != nil) {
		if _,ok := err.(*os.PathError); ok { //Error on the fs, reported as a 500
			logInternalError(w, "Error while updating the job status to '" + status + "'", "Unable to update the status of job '" + j.Id() + "': " + err.Error())
		} else { //Error at the job level, this means the status is not viable
			http.Error(w, err.Error(), http.StatusConflict)
		}
		return
	}
	log.Printf("Job '%s', status set to '%s'\n", j.Id(), status)
}

func GetData(w http.ResponseWriter, r *http.Request, j *Job) {
	dta,err := j.Data()
	if (err != nil) {
		logInternalError(w, fmt.Sprintf("Unable to read the data from the existing job '%s'\n", j.Id()), err.Error())
		return
	}
	w.Write(dta)
}

func GetStatus(w http.ResponseWriter, r *http.Request, j *Job) {
	dta := j.Status()
	w.Write([]byte{byte(dta)})
}


func GetJob(w http.ResponseWriter, r *http.Request, j *Job) {
	w.Header().Set("content-type", "application/json")
	buf := make(map[string]interface {})
	buf["id"] = j.Id()
	buf["status"] = j.Status().String()
	buf["data"] = "http://" + r.Host + "/jobs/" + j.Id() + "/data"
	buf["results"] = mapResults(j, "http://" + r.Host)
	enc := json.NewEncoder(w)
	enc.Encode(buf)
}

func mapResults(j *Job, prefix string) map[string]string {
	rr := make(map[string]string)
	for _, r := range j.Results() {
		rr[r] = prefix + "/jobs/" + j.Id() + "/results/" + r
	}
	return rr
}

func PushJob(w http.ResponseWriter, r *http.Request) {
	jId := mux.Vars(r)["j"]
	cnt, err := ioutil.ReadAll(r.Body)
	if len(cnt) == 0 {
		http.Error(w, "Missing data", http.StatusBadRequest)
		return
	}
	if (err != nil) {
		logInternalError(w, err.Error(), err.Error())
		return
	}
	err = idx.NewJob(jId,cnt)
	if (err != nil) {
		if _,ok := err.(*os.PathError); ok { //Error on the fs, reported as a 500
			logInternalError(w, "Error while creating the job", "Unable to create a job: " + err.Error())
		} else { //Error at the job level, this means the job already exists
			http.Error(w, err.Error(), http.StatusConflict)
		}
		return
	}
	http.Redirect(w, r, "/jobs/" + jId, http.StatusCreated)
	log.Printf("Job '%s' added\n", jId)
}

func GetResult(w http.ResponseWriter, r *http.Request, j *Job) {
	id := mux.Vars(r)["r"]
	for _,k := range j.Results() {
		if (k == id) {
			//The key exists, any error will be due to the fs
			dta,err := j.Result(id)
			if (err != nil) {
				logInternalError(w, fmt.Sprintf("Unable to get the existing result '%s'", id), err.Error())
				return
			}
			w.Write(dta)
			return
		}
	}
	//The key is not here, 404
	http.Error(w, fmt.Sprintf("Result '%s' not found", id), http.StatusNotFound)
}

func PutResult(w http.ResponseWriter, r *http.Request, j *Job) {
	res := mux.Vars(r)["r"]
	cnt, _ := ioutil.ReadAll(r.Body)
	err := j.AddResult(res, cnt)
	if (err != nil) {
		if _,ok := err.(*os.PathError); ok { //Error on the fs, reported as a 500
			logInternalError(w, "Error while storing the result data", err.Error())
		} else { //Error at the job level, this means the result already exists or the state is invalid
			//TODO; If state specific, error 403 (forbidden)

			//TODO: If already exists, error Conflict
			http.Error(w, err.Error(), http.StatusConflict)
		}
	}
}

func GetResults(w http.ResponseWriter, r *http.Request, j *Job) {
	enc:= json.NewEncoder(w)
	w.Header().Set("content-type", "application/json")
	enc.Encode(mapResults(j, "http://" + r.Host))
}

func PopJob(w http.ResponseWriter, r *http.Request) {
	j, err := idx.ProcessFirstReady()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %s", err), http.StatusInternalServerError)
		return
	}
	if (j == nil) {
		http.Error(w, "No jobs are waiting for being processed", http.StatusGone)
		return
	}
	http.Redirect(w, r, "http://" + r.Host + "/jobs/" + j.Id(), http.StatusOK)
	log.Printf("Job '%s' is processing\n", j.Id())

}

func GetJobs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/json")
	enc := json.NewEncoder(w)
	buf := make([]map[string]string, 0)
	for _,id := range idx.ListJobs() {
		j,_ := idx.GetJob(id)
		job := make(map[string]string, 3)
		job["id"] = id
		job["status"] = j.Status().String()
		job["url"] = "http://" + r.Host + "/jobs/" + id
		buf = append(buf, job)
	}
	enc.Encode(buf)
}

