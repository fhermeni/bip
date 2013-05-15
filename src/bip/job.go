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

type StatusError struct {
	Expected JobStatus
	Got JobStatus
}

func (err *StatusError) Error() string {
	return fmt.Sprintf("Expected status updated to '%s'. Got '%s'.", err.Expected.String(), err.Got.String())
}

type Job struct {
	root string
	status JobStatus
	results map[string]bool
	id string
}

func (j *Job) Id() string {
	return j.id
}

func (j *Job) Status() JobStatus {
	return j.status
}

func (j *Job) Results() []string {
	res := make([]string, 0)
	for  id,_ := range j.results {
		res = append(res, id)
	}
	return res
}

func (j *Job) Result(r string) (bool, []byte, error) {
	if (j.results[r]) {
		cnt, err := ioutil.ReadFile(j.root + "/results/" + r)
		return true, cnt, err
	}
	return false, nil, nil
}

func (j *Job) Data() ([]byte, error) {
	return ioutil.ReadFile(j.root + "/data")
}

func (j *Job) AddResult(r string, cnt []byte) error {
	if (j.status != terminating) {
		return fmt.Errorf("Job should be in state 'terminating'\n")
	}
	if (j.results[r]) {
		return fmt.Errorf("Result id '%s' already used", r)
	}
	err := ioutil.WriteFile(j.root + "/results/" + r, cnt, 0600)
	if err == nil {
		j.results[r] = true
	}
	return err
}

func NewJob(root string, id string, data []byte) (*Job, error){
	stat, err := os.Stat(root);
	if (err == nil && (stat != nil && stat.IsDir())) {
		return nil, fmt.Errorf("Error, Job already exists\n")
	}
	if err = os.MkdirAll(root + "/results", 0700); err != nil {
		return nil, err
	}
	j := &Job{root, 0, make(map[string]bool), id}

	if err = j.setStatus(creating); err != nil {
		return nil, err
	}
	if err = ioutil.WriteFile(root + "/data", data, 0600); err != nil {
		return nil, err
	}

	if err = j.setStatus(ready); err != nil {
		return nil, err
	}
	return j, err
}

func ResumeJob(root string, id string) (*Job, error){
	stat, _:= os.Stat(root);
	if (!stat.IsDir()) {
		return nil, fmt.Errorf("directory '%s' does not exist\n ", root)
	}

	status, err := ioutil.ReadFile(root + "/status")
	if (err != nil) {
		return nil, err
	}

	entries, err:= ioutil.ReadDir(root + "/results");
	results := make(map[string]bool)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			results[e.Name()] = true
		}
	}

	j := &Job{root, JobStatus(status[0]), results, id}
	if err = j.setStatus(ready); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *Job) Process() error {
	return j.switchStatus(ready, processing)
}

func (j *Job) Terminating() error {
	return j.switchStatus(processing, terminating)
}

func (j *Job) Terminated() error {
	return j.switchStatus(terminating, terminated)
}

func (j *Job) switchStatus(from, to JobStatus) error {
	if (j.status != from) {
		return &StatusError{from, to}
	}
	return j.setStatus(to)
}

func (j *Job) setStatus(to JobStatus) error {
	if err := ioutil.WriteFile(j.root + "/status", []byte{byte(to)}, 0600); err != nil {
		return err
	}
	j.status = to
	return nil
}

func (j *Job) String() string {
	return j.Id() + j.Status().String();
}





