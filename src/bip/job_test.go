/**
 *
 *
 * @author Fabien Hermenier
 */
package bip

import (
	"testing"
	"io/ioutil"
	"os"
	"bytes"
)

func Test_NewJob(t * testing.T) {
	path, err := ioutil.TempDir("/tmp", "bip")
	if (err != nil) {
		t.Fatal(err)
	}
	cnt := []byte{0,1,2,3,4,5,6,7,8,9,10}
	j, err:= NewJob(path, cnt)
	st := j.Status()
	if ( st != ready) {
		t.Errorf("Status should be 1, instead %d", st)
	}
	dta,_ := j.Data()
	if bytes.Compare(cnt, dta) != 0 {
		t.Errorf("Data differs: got '%x', expected '%x'\n", dta, cnt)
	}
	if (len(j.Results()) != 0) {
		t.Errorf("No results expected. Got %x\n", j.Results())
	}
	defer os.RemoveAll(path);
}

func Test_Process(t * testing.T) {
	path, err := ioutil.TempDir("/tmp", "bip")
	if (err != nil) {
		t.Fatal(err)
	}

	cnt := []byte{0,1,2,3,4,5,6,7,8,9,10}
	j, err:= NewJob(path, cnt)
	err = j.Process()
	if (err != nil) {
		t.Fatal(err)
	}

	st := j.Status()
	if ( st != processing) {
		t.Errorf("Status should be 'processing', but got %s", st)
	}

	err = j.Process()
	if (err == nil) {
		t.Fatal("Process() should not be successfull\n")
	}
	defer os.RemoveAll(path);
}
