package main

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	humanize "github.com/dustin/go-humanize"
	"github.com/joho/godotenv"
	"golang.org/x/sync/errgroup"
)

// Part defines part information
type Part struct {
	Number int
	Buffer []byte
}

const (
	maxPartSize = int64(5242880) // S3 Minimum
	maxRetries  = 3
	maxWorkers  = 10
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() / 2) // http://ascii.jp/elem/000/001/480/1480872/
}

func main() {
	err := godotenv.Load("aws.config")
	if err != nil {
		log.Fatal("Error loading .aws.config file")
	}

	awsAccessKeyID := os.Getenv("S3_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("S3_SECRET_ACCESS_KEY")
	awsBucketRegion := os.Getenv("S3_BUCKET_REGION")
	awsBucketName := os.Getenv("S3_BUCKET_NAME")
	awsEndpoint := os.Getenv("S3_ENDPOINT")

	path := os.Args[1]

	creds := credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, "")
	_, err = creds.Get()
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

	fi, _ := f.Stat()
	size := fi.Size()
	buf := make([]byte, size)
	ct := http.DetectContentType(buf)
	f.Read(buf)

	log.Printf("Upload size is %s\n", humanize.Bytes(uint64(size)))

	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(awsBucketName),
		Key:         aws.String(path),
		ContentType: aws.String(ct),
	}

	sTime := time.Now()

	resp, err := svc.CreateMultipartUpload(input)
	if err != nil {
		log.Println(err.Error())
		return
	}
	log.Println("Created multipart upload request")

	// Build parts
	num := size / maxPartSize
	if size%maxPartSize != 0 {
		num++
	}
	var parts = make([]Part, num)
	var current, length int64
	var remaining = size
	partNumber := 1
	for current = 0; remaining != 0; current += length {
		if remaining < maxPartSize {
			length = remaining
		} else {
			length = maxPartSize
		}

		parts[partNumber-1] = Part{
			Number: partNumber,
			Buffer: buf[current : current+length],
		}

		remaining -= length
		partNumber++
	}

	log.Println("Total number of parts:", partNumber-1)

	q := make(chan Part, partNumber-1)

	// var completedParts []*s3.CompletedPart
	var completedParts = make([]*s3.CompletedPart, num)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	eg := errgroup.Group{}

	// Init workers
	for i := 0; i < maxWorkers; i++ {
		eg.Go(func() error {
			for {
				part, ok := <-q
				if !ok {
					return nil
				}
				completedPart, err := uploadPart(svc, resp, part.Buffer, part.Number)
				if err != nil {
					log.Println(err.Error())
					err := abortMultipartUpload(svc, resp)
					if err != nil {
						log.Println(err.Error())
					}
					return err
				}
				// completedParts = append(completedParts, completedPart)
				completedParts[part.Number-1] = completedPart
			}
		})
	}

	// Queuing
	for _, part := range parts {
		q <- part
	}

	close(q)

	cancel()

	if err := eg.Wait(); err != nil {
		log.Println("Error:", err.Error())
		return
	}

	// PartNumber で並び替えしていないとエラーになるので対処
	// sort.Slice(completedParts, func(i, j int) bool {
	// 	return *completedParts[i].PartNumber < *completedParts[j].PartNumber
	// })

	completeResponse, err := completeMultipartUpload(svc, resp, completedParts)
	if err != nil {
		log.Println(err.Error())
		return
	}

	log.Println("Successfully uploaded file:", completeResponse.String())
	dTime := time.Now()
	duration := dTime.Sub(sTime)
	log.Println("Elapsed time:", duration)
}

func completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(completeInput)
}

func uploadPart(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	for tryNum <= maxRetries {
		uploadResult, err := svc.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return nil, aerr
				}
				return nil, err
			}
			log.Printf("Retrying to upload part #%v\n", partNumber)
			tryNum++
		} else {
			log.Printf("Uploaded part #%v\n", partNumber)
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

func abortMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	log.Println("Aborting multipart upload for UploadId#" + *resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := svc.AbortMultipartUpload(abortInput)
	return err
}
