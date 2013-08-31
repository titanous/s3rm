package main

import (
	"log"
	"os"

	"github.com/titanous/goamz/aws"
	"github.com/titanous/goamz/s3"
)

func main() {
	auth := aws.Auth{os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")}
	bucket := s3.New(auth, aws.USEast).Bucket(os.Args[1])
	var marker string
	for i := 0; true; i++ {
		var prefix string
		if len(os.Args) > 2 {
			prefix = os.Args[2]
		}
		list, err := bucket.List(prefix, "", marker, 1000)
		if err != nil {
			log.Fatalln("list:", err)
		}

		keys := make([]string, len(list.Contents))
		for i, k := range list.Contents {
			keys[i] = k.Key
		}
		errs := bucket.DeleteMulti(keys)
		if errs != nil {
			for k, e := range errs {
				log.Println(k, e)
			}
			if len(errs) == len(keys) || keys["all"] != nil {
				os.Exit(1)
			}
		}
		log.Printf("Deleted batch %d", i)
		if !list.IsTruncated {
			break
		}
		marker = keys[len(keys)-1]
	}
}
