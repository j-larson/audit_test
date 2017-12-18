package main

import (
	"fmt"
	"time"
	"sync/atomic"

	adt "github.com/couchbase/goutils/go-cbaudit"
)

type n1qlAuditEvent struct {
	adt.GenericFields

	RequestId      string            `json:"requestId"`
	Statement      string            `json:"statement"`
	NamedArgs      map[string]string `json:"namedArgs,omitempty"`
	PositionalArgs []string          `json:"positionalArgs,omitempty"`

	IsAdHoc   bool   `json:"isAdHoc"`
	UserAgent string `json:"userAgent"`
	Node      string `json:"node"`

	Status string `json:"status"`

	Metrics *n1qlMetrics `json:"metrics"`
}

type n1qlMetrics struct {
	ElapsedTime   string `json:"elapsedTime"`
	ExecutionTime string `json:"executionTime"`
	ResultCount   int    `json:"resultCount"`
	ResultSize    int    `json:"resultSize"`
	MutationCount uint64 `json:"mutationCount,omitempty"`
	SortCount     uint64 `json:"sortCount,omitempty"`
	ErrorCount    int    `json:"errorCount,omitempty"`
	WarningCount  int    `json:"warningCount,omitempty"`
}

var totalSubmitted uint64
var numSuccess uint64
var numFailure uint64

func submitRequest(svc *adt.AuditSvc, i int, num int) {
	record := &n1qlAuditEvent { Statement: fmt.Sprintf("audit %d of %d", i, num) }
	err := svc.Write(28672, record)
	if err == nil {
		atomic.AddUint64(&numSuccess, 1)
	} else {
		atomic.AddUint64(&numFailure, 1)
		fmt.Printf("Unable to submit request %d of %d: %s", i, num, err.Error())
	}
	atomic.AddUint64(&totalSubmitted, 1)
}

func main() {
	service, err := adt.NewAuditSvc("http://127.0.0.1:8091")
	if err != nil {
		fmt.Printf("Unable to start audit service: %s\n", err.Error())
		return
	}

	numMsg := 10000

	for i:= 0; i < numMsg; i++ {
		go submitRequest(service, i, numMsg)
	}

	for {
		numSubmitted := atomic.LoadUint64(&totalSubmitted)
		fmt.Printf("finished %d submits of %d\n", numSubmitted, numMsg)
		if int(numSubmitted) < numMsg {
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	fmt.Printf("successes %d failures %d\n", atomic.LoadUint64(&numSuccess), atomic.LoadUint64(&numFailure))
}
