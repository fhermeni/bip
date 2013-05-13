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
)

func StartREST(i Index, port int) error {
	idx = i
	r := mux.NewRouter()
	r.HandleFunc("/jobs/", GetJobs).Methods("GET")
	r.HandleFunc("/jobs/", ProcessJob).Methods("PUT")
	r.HandleFunc("/jobs/{j}/data", GetData).Methods("GET")
	r.HandleFunc("/jobs/{j}", AddJob).Methods("POST")
	r.HandleFunc("/jobs/{j}", GetJob).Methods("GET")
	r.HandleFunc("/jobs/{j}/results/{r}", GetResult).Methods("GET")
	r.HandleFunc("/jobs/{j}/results/", GetResults).Methods("GET")
	http.Handle("/", r)
	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

var idx Index

func toURL(rc string, r *http.Request) string {
	return "http://" + r.Host + rc
}
func GetData(w http.ResponseWriter, r *http.Request) {
	j, ok := idx.GetJob(mux.Vars(r)["j"])
	if !ok {
		http.Error(w, fmt.Sprintf("Job '%s' not found", mux.Vars(r)["j"]), http.StatusNotFound)
		return
	}
	dta,_ := j.Data()
	w.Write(dta)
}

func GetJob(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["j"]
	j, ok := idx.GetJob(id)

	if !ok {
		http.Error(w, fmt.Sprintf("Job '%s' not found", id), http.StatusNotFound)
		return
	}
	w.Header().Set("content-type", "application/json")
	buf := make(map[string]interface {})
	buf["id"] = id
	buf["status"] = j.Status().String()
	buf["data"] = toURL("/jobs/" + id + "/data", r)
	buf["results"] = mapResults(j, id, "http://" + r.Host)
	enc := json.NewEncoder(w)
	enc.Encode(buf)
}

func mapResults(j *Job, id string, prefix string) map[string]string {
	rr := make(map[string]string)
	for _, r := range j.Results() {
		rr[r] = prefix + "/jobs/" + id + "/results/" + r
	}
	return rr
}

func AddJob(w http.ResponseWriter, r *http.Request) {
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
		fmt.Fprintf(w, "Unable to add the job: %s\n", err)
	}
	http.Redirect(w, r, "/jobs/" + jId, http.StatusCreated)
}

func GetResult(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["j"]
	j, ok := idx.GetJob(id)
	if !ok {
		if !ok {
			http.Error(w, fmt.Sprintf("Job '%s' not found", id), http.StatusNotFound)
			return
		}
	}
	dta,_ := j.Result(id)
	w.Write(dta)
}

func GetResults(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["j"]
	j, ok := idx.GetJob(id)
	if !ok {
		http.Error(w, fmt.Sprintf("Job '%s' not found", id), http.StatusNotFound)
		return
	}
	enc:= json.NewEncoder(w)
	w.Header().Set("content-type", "application/json")
	enc.Encode(mapResults(j, id, "http://" + r.Host))
}

func ProcessJob(w http.ResponseWriter, r *http.Request) {
	j, err := idx.ProcessFirstReady()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %s", err), http.StatusInternalServerError)
		return
	}
	if (j == nil) {
		http.Error(w, "No jobs are waiting for being processed", http.StatusGone)
		return
	}
	http.Redirect(w, r, toURL("/jobs/" + j.Id(), r), http.StatusOK)

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
		job["url"] = toURL("/jobs/" + id, r)
		buf = append(buf, job)
	}
	enc.Encode(buf)
}

