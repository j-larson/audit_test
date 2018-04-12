package main

import (
	"fmt"
	"net/http"
	"io/ioutil"
	"flag"
	"time"
)

func main() {
	target := flag.String("target", "http://172.23.123.101:8093", "query service URL")
	num := flag.Int("num", 3, "number of requests to send for each worker")
	par := flag.Int("par", 5, "number of worker threads")
	showQueries := flag.Bool("showQueries", false, "show queries")
	sendQueries := flag.Bool("sendQueries", true, "send queries")
	showResults := flag.Bool("showResults", false, "show results")

	flag.Parse()

	fmt.Printf("target: %s\n", *target)
	fmt.Printf("par: %d\n", *par)
	fmt.Printf("num: %d\n", *num)
	fmt.Printf("showQueries: %v\n", *showQueries)
	fmt.Printf("sendQueries: %v\n", *sendQueries)
	fmt.Printf("showResults: %v\n", *showResults)

	doneChan := make(chan bool)


	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    1 * time.Second,
		DisableKeepAlives:  true,
		DisableCompression: true,
	}
	client := &http.Client{Transport: tr}

	for i:=0; i < *par; i++ {
		go doWorker(i+1, *target, *num, *showQueries, *sendQueries, *showResults, doneChan, client)
	}
	for i:=0; i < *par; i++ {
		_ = <- doneChan
	}
}

func doWorker(workerNum int, target string, num int, showQueries bool, sendQueries bool, showResults bool, doneChan chan bool, client *http.Client) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Worker %d failed. %v\n", workerNum, r)
			doneChan <- false
		}
	}()
	totalElapsed := time.Duration(0)
	totalErrors := 0

	query := target + "/admin/ping"
	for i := 0; i < num; i++ {
		if i %1000 == 0 {
			fmt.Printf("Worker %d: Running query %d\n", workerNum, i)
		}
		if showQueries {
			fmt.Printf("Worker %d: Query %d: %s\n", workerNum, i, query)
		}
		req, err := http.NewRequest("GET", query, nil)
		if err != nil {
			fmt.Printf("Worker %d: Unable to create request: %v\n", workerNum, err)
			totalErrors++
			continue
		}
		req.SetBasicAuth("Administrator", "password")
		
		startTime := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Worker %d: Request failed: %v\n", workerNum, err)
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
			totalErrors++
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("Worker %d: Unable to read response body: %v", workerNum, err)
			totalErrors++
			continue
		}
		totalElapsed = totalElapsed + time.Since(startTime)
		if showResults {
			fmt.Printf("Worker %d: %s\n", workerNum, body)
		}
	}
	
	fmt.Printf("Worker %d: Total elapsed (ms): %v\n", workerNum, totalElapsed.Seconds()*1000.0)
	fmt.Printf("Worker %d: Total errors: %v\n", workerNum, totalErrors)
	doneChan <- true
}

