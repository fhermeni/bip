/**
 *
 *
 * @author Fabien Hermenier
 */
package bip

import (
	"os"
	"io/ioutil"
)

type Index struct {
	jobs map[string]*Job
	root string
}


func NewIndex(root string) (*Index, error) {

	idx := &Index{make(map[string]*Job), root}
	stat, err := os.Stat(root)
	if (err != nil) {
		err = os.MkdirAll(root, 0700)
		if err != nil {
			return nil, err
		}
	} else if (stat.IsDir()) {
		cnt, err := ioutil.ReadDir(root)
		if (err != nil) {
			return nil, err
		}
		for _,e := range cnt {
			stat, err = os.Stat(root + "/" + e.Name())
			if (err == nil && stat.IsDir()) {
				err = idx.addJob(e.Name());
				if (err != nil) {
					return nil, err
				}
			}
		}
	}
	return idx, nil
}

func (idx * Index) ListJobs() []string {
	keys := make([]string, 0)
	for k,_ := range(idx.jobs) {
		keys = append(keys, k)
	}
	return keys
}

func (idx * Index) GetJob(id string) (*Job, bool) {
	j, ok := idx.jobs[id]
	return j, ok
}

func (idx * Index) addJob(id string) error {
	j,err := ResumeJob(idx.root + "/" + id, id)
	if (err != nil) {
		return err
	}
	idx.jobs[id] = j
	return nil
}

func (idx * Index) NewJob(id string, data []byte) error {
	j,err := NewJob(idx.root + "/" + id, id, data)
	if (err != nil) {
		return err
	}
	idx.jobs[id] = j
	return nil
}

func (idx * Index) ProcessFirstReady() (*Job, error) {
	for _,j := range idx.jobs {
		if (j.Status() == ready) {
			err := j.Process()
			return j, err
		}
	}
	return nil, nil
}

