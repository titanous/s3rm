package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/titanous/goamz/aws"
	"github.com/titanous/goamz/s3"
)

var batches = make(chan []string)
var logs = make(chan string)
var bucket *s3.Bucket
var workers = flag.Int("workers", 10, "")
var done sync.WaitGroup

func main() {
	flag.Parse()
	auth := aws.Auth{os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")}
	bucket = s3.New(auth, aws.USEast).Bucket(flag.Arg(0))

	go logLines()
	for i := 0; i < *workers; i++ {
		go deleteBatches()
	}
	done.Add(*workers)

	var marker string
	for {
		var prefix string
		if len(os.Args) > 2 {
			prefix = flag.Arg(1)
		}
		list, err := bucket.List(prefix, "", marker, 1000)
		if err != nil {
			log.Fatalln("list:", err)
		}
		if len(list.Contents) == 0 {
			break
		}

		keys := make([]string, len(list.Contents))
		for i, k := range list.Contents {
			keys[i] = k.Key
		}

		batches <- keys

		if !list.IsTruncated {
			break
		}
		marker = keys[len(keys)-1]
	}
	close(batches)
	done.Wait()
}

func logLines() {
	for line := range logs {
		log.Println(line)
	}
}

func deleteBatches() {
	for batch := range batches {
		errs := bucket.DeleteMulti(batch)
		if errs != nil {
			for k, e := range errs {
				logs <- fmt.Sprint(k, e)
			}
		}
		logs <- "Deleted batch"
	}
	done.Done()
}
