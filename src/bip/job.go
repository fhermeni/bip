/**
 *
 *
 * @author Fabien Hermenier
 */
package bip

import (
	"io/ioutil"
	"os"
	"fmt"
)

type JobStatus byte

const (
	creating = 0
	ready = 1
	processing = 2
	terminating = 3
	terminated = 4
)

func (s JobStatus) String() string {
	switch(s) {
		case 0: return fmt.Sprintf("creating")
		case 1: return fmt.Sprintf("ready")
		case 2: return fmt.Sprintf("processing")
		case 3: return fmt.Sprintf("terminating")
		case 4: return fmt.Sprintf("terminated")
	}
	return fmt.Sprintf("%d", s)
}

type Job struct {
	root string
	status JobStatus
	results []string
	id string
}

func (j *Job) Id() string {
	return j.id
}

func (j *Job) Status() JobStatus {
	return j.status
}

func (j *Job) Results() []string {
	return j.results
}

func (j *Job) Result(r string) ([]byte, error) {
	return ioutil.ReadFile(j.root + "/results/" + r)
}

func (j *Job) Data() ([]byte, error) {
	return ioutil.ReadFile(j.root + "/data")
}

func (j *Job) AddResult(r string, cnt []byte) error {
	if (j.status != processing && j.status != terminating) {
		return fmt.Errorf("Job should be in state 'processing' or 'terminating'\n")
	}
	if (j.status == processing) {
		if err := ioutil.WriteFile(j.root + "/status", []byte{terminating}, 0600); err != nil {
			return err
		}
		j.status = terminating
	}
	if _,err := os.Stat(j.root + "/results/" + r); err != nil	{
		return fmt.Errorf("Id '%s' already used", r);
	}
	return ioutil.WriteFile(j.root + "/results/" + r, cnt, 0600)
}

func NewJob(root string, id string, data []byte) (*Job, error){
	stat, err := os.Stat(root);
	if (err == nil && (stat != nil && stat.IsDir())) {
		return nil, fmt.Errorf("Error, Job already exists\n")
	}
	if err = os.MkdirAll(root + "/results", 0700); err != nil {
		return nil, err
	}
	if err = ioutil.WriteFile(root + "/status", []byte{creating}, 0600); err != nil {
		return nil, err
	}
	if err = ioutil.WriteFile(root + "/data", data, 0600); err != nil {
		return nil, err
	}

	if err = ioutil.WriteFile(root + "/status", []byte{ready}, 0600); err != nil {
		return nil, err
	}
	return &Job{root, 1, make([]string, 0), id}, err
}

func ResumeJob(root string, id string) (*Job, error){
	stat, _:= os.Stat(root);
	if (!stat.IsDir()) {
		return nil, fmt.Errorf("directory '%s' does not exist %b\n ", root)
	}

	status, err := ioutil.ReadFile(root + "/status")
	if (err != nil) {
		return nil, err
	}

	entries, err:= ioutil.ReadDir(root + "/results");
	results := make([]string, len(entries))
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			results = append(results, e.Name())
		}
	}

	if err = ioutil.WriteFile(root + "/status", []byte{ready}, 0600); err != nil {
		return nil, err
	}
	return &Job{root, JobStatus(status[0]), results, id}, nil
}

func (j *Job) Process() error {
	if (j.status != ready) {
		return fmt.Errorf("Job should be in state %s. Currently in state %s\n", ready, j.status)
	}
	if err := ioutil.WriteFile(j.root + "/status", []byte{processing}, 0600); err != nil {
		return err
	}
	j.status = processing
	return nil
}

func (j *Job) Terminated() error {
	if (j.status != processing && j.status != terminating) {
		return fmt.Errorf("Job should be in state 'processing' or 'terminating'\n")
	}
	if err := ioutil.WriteFile(j.root + "/status", []byte{terminated}, 0600); err != nil {
		return err
	}
	return nil
}

func (j *Job) String() string {
	return fmt.Sprintf("%s[%s]", j.Id(), j.Status().String())
}





