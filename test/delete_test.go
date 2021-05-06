package skbn_test

import (
	"log"
	"testing"

	"github.com/Cognologix/skbn/pkg/skbn"
)

func TestDelete(t *testing.T) {
	//Test should create the file before deleting it
	path := "k8s://namespace/pod/container/path/to/delete/from"
	if err := skbn.Delete(path, 1); err != nil {
		log.Fatal(err)
	}
}
