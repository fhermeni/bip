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
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

var idx Index

func StartREST(i Index, port int) error {
	idx = i
	r := mux.NewRouter()
	r.HandleFunc("/jobs/", GetJobs).Methods("GET")
	r.HandleFunc("/jobs/", PopJob).Methods("PUT")
	r.HandleFunc("/jobs/", PushJob).Methods("POST")
	r.HandleFunc("/jobs/{j}", makeJobHandler(GetJob)).Methods("GET")
	r.HandleFunc("/jobs/{j}/data", makeJobHandler(GetData)).Methods("GET")
	r.HandleFunc("/jobs/{j}/status", makeJobHandler(GetStatus)).Methods("GET")
	r.HandleFunc("/jobs/{j}/status", makeJobHandler(UpdateStatus)).Methods("PUT")
	r.HandleFunc("/jobs/{j}/results/", makeJobHandler(GetResults)).Methods("GET")
	r.HandleFunc("/jobs/{j}/results/{r}", makeJobHandler(GetResult)).Methods("GET")
	r.HandleFunc("/jobs/{j}/results/", makeJobHandler(PutResult)).Methods("POST")
	http.Handle("/", r)
	return http.ListenAndServe(":" + strconv.Itoa(port), nil)
}

func logInternalError(w http.ResponseWriter, userMsg , serverMsg string) {
	http.Error(w, userMsg, http.StatusInternalServerError)
	log.Println(serverMsg)
}

func makeJobHandler(fn func(http.ResponseWriter, *http.Request, *Job)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["j"]
		j, ok := idx.GetJob(id)
		if !ok {
			http.Error(w, "Job '" + id + "' not found", http.StatusNotFound)
			return
		}
		fn(w, r, j)
	}
}

func UpdateStatus(w http.ResponseWriter, r * http.Request, j *Job) {
	r.ParseForm()
	s := r.Form.Get("s")
	if s == "" {
		http.Error(w, "Missing required parameter 's' to specify the new status", http.StatusBadRequest)
		return
	}
	var err error
	switch (s) {
		case "processing": err = j.Process()
		case "terminating": err = j.Terminating()
		case "terminated": err = j.Terminated()
		default:
			http.Error(w, "non-viable status code: " + s, http.StatusBadRequest)
			return
	}
	if (err != nil) {
		if _,ok := err.(*os.PathError); ok { //Error on the fs, reported as a 500
			logInternalError(w, "Error while updating the job status to '" + s + "'",
							 "Unable to update the status of job '" + j.Id() + "': " + err.Error())
		} else { //Error at the job level, this means the status is not viable
			http.Error(w, err.Error(), http.StatusConflict)
		}
		return
	}
	log.Printf("Job '%s', status set to '%s'\n", j.Id(), s)
}

func GetData(w http.ResponseWriter, r *http.Request, j *Job) {
	dta,err := j.Data()
	if (err != nil) {
		logInternalError(w, "Unable to read the data from the existing job '" + j.Id() + "'", err.Error())
		return
	}
	w.Write(dta)
}

func GetStatus(w http.ResponseWriter, r *http.Request, j *Job) {
	dta := j.Status()
	w.Write([]byte(dta.String()))
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
	r.ParseForm()
	jId := r.Form.Get("j")
	if jId == "" {
		http.Error(w, "Missing required parameter 'j' to declare the job identifier", http.StatusBadRequest)
		return
	}
	cnt, err := ioutil.ReadAll(r.Body)
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
	ok, cnt, err := j.Result(id)
	if (!ok) {
		http.Error(w, "Result '" + id + "' not found", http.StatusNotFound)
		return
	}
	if (err != nil) {
		logInternalError(w, "Unable to get the existing result '" + id + "'", err.Error())
		return
	}
	w.Write(cnt)
}

func PutResult(w http.ResponseWriter, r *http.Request, j *Job) {
	r.ParseForm()
	res := r.Form.Get("r")
	if res == "" {
		http.Error(w, "Missing required parameter 'r' to declare the result identifier", http.StatusBadRequest)
		return
	}
	cnt, err := ioutil.ReadAll(r.Body)
	if (err != nil) {
		logInternalError(w, "Unable to read the result data", err.Error())
		return
	}
	err = j.AddResult(res, cnt)
	if (err != nil) {
		if _,ok := err.(*os.PathError); ok { //Error on the fs, reported as a 500
			logInternalError(w, "Error while storing the result data", err.Error())
		} else  {
			//The job already exists or the current status is incorrect
			http.Error(w, err.Error(), http.StatusConflict)
		}
	}  else {
		http.Redirect(w, r, "/jobs/" + j.Id() + "/results/" + res, http.StatusCreated)
		log.Printf("Job '%s': result '%s' added\n", j.Id(), res)
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
		logInternalError(w, "Error while getting a proccessable job", "Error while getting a proccessable job" + err.Error() + "\n")
	} else if (j == nil) {
		http.Error(w, "No jobs are waiting for being processed", http.StatusNoContent)
	} else {
		http.Redirect(w, r, "http://" + r.Host + "/jobs/" + j.Id(), http.StatusFound)
		log.Printf("Job '%s' is processing\n", j.Id())
	}
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

