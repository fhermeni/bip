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

type JobError struct {
	Type byte //0 for status type, //1 for id problem, //2 for the persistent stuff
	Err error
}

func (err *JobError) Error() string {
	return err.Err.Error()
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

func (j *Job) Result(r string) ([]byte, error) {
	return ioutil.ReadFile(j.root + "/results/" + r)
}

func (j *Job) Data() ([]byte, error) {
	return ioutil.ReadFile(j.root + "/data")
}

func (j *Job) AddResult(r string, cnt []byte) error {
	if (j.status != terminating) {
		return fmt.Errorf("Job should be in state 'terminating'\n")
	}
	if _,err := os.Stat(j.root + "/results/" + r); err != nil	{
		return fmt.Errorf("Id '%s' already used", r)
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
		return fmt.Errorf("Expect status '%s'. Got '%s'\n", from.String(), to.String())
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





