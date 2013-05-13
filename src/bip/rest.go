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
	r.HandleFunc("/jobs/{j}/status", makeJobHandler(Commit)).Methods("PUT")
	r.HandleFunc("/jobs/{j}/results/", makeJobHandler(GetResults)).Methods("GET")
	r.HandleFunc("/jobs/{j}/results/{r}", makeJobHandler(GetResult)).Methods("GET")
	r.HandleFunc("/jobs/{j}/results/{r}", makeJobHandler(PutResult)).Methods("PUT")
	http.Handle("/", r)
	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

var idx Index

func logInternalError(w http.ResponseWriter, userMsg , serverMsg string) {
	http.Error(w, userMsg, http.StatusInternalServerError)
	log.Fatalln(serverMsg)
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

func Commit(w http.ResponseWriter, r * http.Request, j *Job) {
	err := j.Terminated()
	if (err != nil) {
		http.Error(w,fmt.Sprintf("%s", err), http.StatusNotAcceptable)
		return
	}
	log.Printf("Job '%s' committed\n", j.Id())
}

func GetData(w http.ResponseWriter, r *http.Request, j *Job) {
	dta,_ := j.Data()
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
		fmt.Fprintf(w, "Unable to create the job: %s\n", err)
		fmt.Printf("%s\n", err)
		return
	}
	err = idx.NewJob(jId,cnt)
	if (err != nil) {
		http.Error(w, "Unable to add the job: %s\n", http.StatusConflict)
	}
	http.Redirect(w, r, "/jobs/" + jId, http.StatusCreated)
	log.Printf("Job '%s' added\n", jId)
}

func GetResult(w http.ResponseWriter, r *http.Request, j *Job) {
	id := mux.Vars(r)["r"]
	dta,_ := j.Result(id)
	w.Write(dta)
}

func PutResult(w http.ResponseWriter, r *http.Request, j *Job) {
	res := mux.Vars(r)["r"]
	cnt, _ := ioutil.ReadAll(r.Body)
	err := j.AddResult(res, cnt)
	if (err != nil) {
		http.Error(w, fmt.Sprintf("%s",err), http.StatusInternalServerError)
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

