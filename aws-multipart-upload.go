package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cenkalti/backoff"
	humanize "github.com/dustin/go-humanize"
	"github.com/joho/godotenv"
	"golang.org/x/sync/errgroup"
)

// Part defines part information
type Part struct {
	number  int
	content []byte
}

const (
	maxPartSize = int64(5242880) // S3 Minimum (5MB)
	maxRetries  = 3
	maxWorkers  = 10
)

var awsAccessKeyID string
var awsSecretAccessKey string
var awsBucketRegion string
var awsBucketName string
var awsEndpoint string

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() / 2) // http://ascii.jp/elem/000/001/480/1480872/
}

func main() {
	err := godotenv.Load("aws.config")
	if err != nil {
		log.Fatal("Error loading .aws.config file")
	}

	awsAccessKeyID = os.Getenv("S3_ACCESS_KEY_ID")
	awsSecretAccessKey = os.Getenv("S3_SECRET_ACCESS_KEY")
	awsBucketRegion = os.Getenv("S3_BUCKET_REGION")
	awsBucketName = os.Getenv("S3_BUCKET_NAME")
	awsEndpoint = os.Getenv("S3_ENDPOINT")

	upload(os.Args[1])
}

func upload(path string) {
	creds := credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, "")
	_, err := creds.Get()
	if err != nil {
		log.Println("Bad credentials:", err)
	}
	cfg := aws.NewConfig().WithCredentials(creds).WithRegion(awsBucketRegion).WithEndpoint(awsEndpoint)
	svc := s3.New(session.New(), cfg)

	f, err := os.Open(path)
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer f.Close()

	stat, _ := f.Stat()
	size := stat.Size()
	content := make([]byte, size)
	f.Read(content)

	log.Printf("Upload size is %s\n", humanize.Bytes(uint64(size)))

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(awsBucketName),
		Key:         aws.String(path),
		ContentType: aws.String(http.DetectContentType(content)),
	}

	sTime := time.Now()

	resp, err := svc.CreateMultipartUpload(input)
	if err != nil {
		log.Println(err.Error())
		return
	}

	log.Println("Created multipart upload request")

	parts := divideFileIntoParts(content)
	length := len(parts)

	q := make(chan Part, length)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	eg := errgroup.Group{}

	var completedParts = make([]*s3.CompletedPart, length)

	// Booting workers
	for i := 0; i < maxWorkers; i++ {
		eg.Go(func() error {
			awsctx := aws.BackgroundContext()
			for {
				select {
				case <-ctx.Done():
					log.Println("Worker canceled")
					return fmt.Errorf("Worker canceled")
				default:
					part, ok := <-q
					if !ok {
						return nil
					}
					completedPart, err := uploadPart(awsctx, svc, resp, part.content, part.number)
					if err != nil {
						log.Println(err.Error())
						err := abortMultipartUpload(svc, resp)
						if err != nil {
							log.Println(err.Error())
						}
						return err
					}
					completedParts[part.number-1] = completedPart
				}
			}
		})
	}

	// Queuing
	for _, part := range parts {
		q <- part
	}

	close(q)

	// go func() {
	// 	time.Sleep(8 * time.Second)
	// 	cancel()
	// }()

	if err := eg.Wait(); err != nil {
		log.Println(err)
		cancel()
		return
	}

	cancel()

	completeResponse, err := completeMultipartUpload(svc, resp, completedParts)
	if err != nil {
		log.Println(err.Error())
		return
	}

	log.Println("Successfully uploaded file:", completeResponse.String())

	eTime := time.Now()
	duration := eTime.Sub(sTime)
	log.Println("Elapsed time:", duration)
}

func divideFileIntoParts(content []byte) []Part {
	contentSize := int64(len(content))

	size := contentSize / maxPartSize
	if contentSize%maxPartSize != 0 {
		size++
	}

	var parts = make([]Part, size)
	var current, length int64
	var remaining = contentSize
	number := 1

	for current = 0; remaining != 0; current += length {
		if remaining < maxPartSize {
			length = remaining
		} else {
			length = maxPartSize
		}

		parts[number-1] = Part{
			number:  number,
			content: content[current : current+length],
		}

		remaining -= length
		number++
	}

	log.Println("Total number of parts:", size)

	return parts
}

func uploadPart(ctx aws.Context, svc *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	var partOutput *s3.UploadPartOutput
	operation := func() error {
		var err error
		partOutput, err = svc.UploadPartWithContext(ctx, partInput)
		return err
	}

	notify := func(err error, duration time.Duration) {
		log.Printf("%v %v\n", duration, err)
	}

	// err := backoff.Retry(operation, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxRetries))
	// if err != nil {
	// 	log.Println(err)
	// 	return nil, err
	// }

	err := backoff.RetryNotify(
		operation,
		backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxRetries),
		notify)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	log.Printf("Uploaded part #%v\n", partNumber)

	return &s3.CompletedPart{
		ETag:       partOutput.ETag,
		PartNumber: aws.Int64(int64(partNumber)),
	}, nil
}

func completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	input := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	awsctx := aws.BackgroundContext()
	return svc.CompleteMultipartUploadWithContext(awsctx, input)
}

func abortMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	log.Println("Aborting multipart upload for UploadId#" + *resp.UploadId)
	input := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	awsctx := aws.BackgroundContext()
	_, err := svc.AbortMultipartUploadWithContext(awsctx, input)
	return err
}
