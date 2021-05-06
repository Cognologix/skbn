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

func TestSync(t *testing.T) {
	//Test should create the file before deleting it
	src := "s3://bucket/path/to/copy/to"
	dst := "k8s://namespace/pod/container/path/to/copy/from"
	parallel := 1     // one file at a time
	bufferSize := 1.0 // 1GB of in memory buffer size

	if err := skbn.Sync(src, dst, parallel, bufferSize); err != nil {
		log.Fatal(err)
	}

}
