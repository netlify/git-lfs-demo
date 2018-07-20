package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const defaultRegion = "us-west-2"

type awsFn func(srv *s3.S3, o objectRequest) *request.Request

func newS3Service() (*s3.S3, error) {
	keyID := os.Getenv("LFS_AWS_KEY_ID")
	keySecret := os.Getenv("LFS_AWS_KEY_SECRET")

	if keyID == "" {
		return nil, errors.New("Missing key ID")
	}

	if keySecret == "" {
		return nil, errors.New("Missing key Secret")
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(defaultRegion),
		Credentials: credentials.NewStaticCredentials(keyID, keySecret, ""),
	},
	)

	if err != nil {
		return nil, err
	}

	// Create S3 service client
	return s3.New(sess), nil
}

func signObjectUploads(ctx context.Context, transfer string, objects []objectRequest) (events.APIGatewayProxyResponse, error) {
	fn := func(srv *s3.S3, o objectRequest) *request.Request {
		req, _ := srv.PutObjectRequest(&s3.PutObjectInput{
			Bucket: aws.String("netlify-lfs"),
			Key:    aws.String(o.Oid),
		})

		req.HTTPRequest.Header.Set("X-Amz-Acl", "public-read")
		req.HTTPRequest.Header.Set("Content-Length", strconv.FormatInt(o.Size, 10))
		return req
	}
	return signObjects(ctx, transfer, "upload", objects, fn)
}

func signObjectDownloads(ctx context.Context, transfer string, objects []objectRequest) (events.APIGatewayProxyResponse, error) {
	fn := func(srv *s3.S3, o objectRequest) *request.Request {
		req, _ := srv.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String("netlify-lfs"),
			Key:    aws.String(o.Oid),
		})
		return req
	}
	return signObjects(ctx, transfer, "download", objects, fn)
}

func signObjects(ctx context.Context, transfer, actionName string, objects []objectRequest, fn awsFn) (events.APIGatewayProxyResponse, error) {
	svc, err := newS3Service()
	if err != nil {
		return newResponseError(ctx, 500, err)
	}

	resp := batchResponse{
		Transfer: transfer,
		Objects:  make([]objectResponse, len(objects)),
	}

	var verifyAction *action
	if actionName == "upload" {
		lc, ok := lambdacontext.FromContext(ctx)
		if !ok {
			return newResponseError(ctx, 500, errors.New("Unable to get client context"))
		}

		siteURL := lc.ClientContext.Env["site_url"]
		if siteURL == "" {
			siteURL = "https://elegant-turing-8674fb.netlify.com"
		}
		fmt.Println(lc.ClientContext.Env)
		fmt.Println(siteURL)
		verifyAction = &action{
			Href: fmt.Sprintf("%s/.netlify/functions/lfs/verify", siteURL),
		}
	}

	for i, o := range objects {
		ro := objectResponse{
			Oid:           o.Oid,
			Size:          o.Size,
			Authenticated: true,
		}

		d := 15 * time.Minute
		req := fn(svc, o)
		str, err := req.Presign(d)

		if err != nil {
			ro.Error = &responseError{
				Message: err.Error(),
			}
		} else {
			h := map[string]string{
				"X-Amz-Acl": "public-read",
			}
			ro.Actions = map[string]action{
				actionName: action{
					Href:      str,
					Header:    h,
					ExpiresIn: d.Seconds(),
				},
			}

			if verifyAction != nil {
				ro.Actions["verify"] = *verifyAction
			}
		}

		resp.Objects[i] = ro
	}

	b, err := json.Marshal(resp)
	if err != nil {
		return newResponseError(ctx, 500, err)
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(b),
		Headers: map[string]string{
			"Content-Type": "application/vnd.git-lfs+json",
		},
	}, nil
}

func verifyObject(ctx context.Context, o objectRequest) (events.APIGatewayProxyResponse, error) {
	svc, err := newS3Service()
	if err != nil {
		return newResponseError(ctx, 500, err)
	}

	obj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("netlify-lfs"),
		Key:    aws.String(o.Oid),
	})

	if obj != nil && obj.ContentLength != nil && *obj.ContentLength == o.Size {
		return events.APIGatewayProxyResponse{
			StatusCode: 200,
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 404,
	}, nil
}
