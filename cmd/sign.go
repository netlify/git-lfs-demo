package main

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type awsFn func(srv *s3.S3, o objectRequest) *request.Request

func signObjectUploads(ctx context.Context, transfer string, objects []objectRequest) (events.APIGatewayProxyResponse, error) {
	fn := func(srv *s3.S3, o objectRequest) *request.Request {
		req, _ := srv.PutObjectRequest(&s3.PutObjectInput{
			Bucket: aws.String("lfs"),
			Key:    aws.String(o.Oid),
		})
		req.HTTPRequest.Header.Set("Content-Length", strconv.FormatUint(o.Size, 10))
		return req
	}
	return signObjects(ctx, transfer, "upload", objects, fn)
}

func signObjectDownloads(ctx context.Context, transfer string, objects []objectRequest) (events.APIGatewayProxyResponse, error) {
	fn := func(srv *s3.S3, o objectRequest) *request.Request {
		req, _ := srv.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String("lfs"),
			Key:    aws.String(o.Oid),
		})
		return req
	}
	return signObjects(ctx, transfer, "download", objects, fn)
}

func signObjects(ctx context.Context, transfer, actionName string, objects []objectRequest, fn awsFn) (events.APIGatewayProxyResponse, error) {
	keyID := os.Getenv("LFS_AWS_KEY_ID")
	keySecret := os.Getenv("LFS_AWS_KEY_SECRET")

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: credentials.NewStaticCredentials(keyID, keySecret, "TOKEN"),
	},
	)

	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       err.Error(),
		}, err
	}

	// Create S3 service client
	svc := s3.New(sess)

	resp := batchResponse{
		Transfer: transfer,
		Objects:  make([]objectResponse, len(objects)),
	}

	for i, o := range objects {
		ro := objectResponse{
			Oid:           o.Oid,
			Size:          o.Size,
			Authenticated: true,
		}

		d := 15 * time.Minute
		req := fn(svc, o)
		req.ExpireTime = d

		if err := req.Sign(); err != nil {
			ro.Error = &responseError{
				Message: err.Error(),
			}
		} else {
			h := map[string]string{}
			for k, v := range req.SignedHeaderVals {
				h[k] = v[0]
			}
			ro.Actions = map[string]action{
				actionName: action{
					Href:      req.HTTPRequest.URL.String(),
					Header:    h,
					ExpiresIn: d.Seconds(),
				},
			}
		}

		resp.Objects[i] = ro
	}

	b, _ := json.Marshal(resp)
	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(b),
	}, nil
}
