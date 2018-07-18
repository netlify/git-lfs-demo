package main

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambdacontext"
)

// // POST https://lfs-server.com/objects/batch
// Accept: application/vnd.git-lfs+json
// Content-Type: application/vnd.git-lfs+json
// Authorization: Basic ... (if needed)
//{
//	"operation": "download",
//    "transfers": [ "basic" ],
//    "ref": { "name": "refs/heads/master" },
//    "objects": [
//	    {
//			"oid": "12345678",
//		    "size": 123,
//		}
//	]
//}

type ref struct {
	Name string `json:"name"`
}

type action struct {
	Href      string            `json:"href"`
	Header    map[string]string `json:"header"`
	ExpiresIn float64           `json:"expires_in,omitempty"`
	ExpiresAt string            `json:"expires_at,omitempty"`
}

type objectRequest struct {
	Oid  string `json:"oid"`
	Size uint64 `json:"size"`
}

type objectResponse struct {
	Oid           string            `json:"oid"`
	Size          uint64            `json:"size"`
	Authenticated bool              `json:"authenticated"`
	Actions       map[string]action `json:"actions,omitempty"`
	Error         *responseError    `json:"error,omitempty"`
}

type batchRequest struct {
	Operation string          `json:"operation"`
	Transfers []string        `json:"transfers"`
	Ref       *ref            `json:"ref,omitemtpy"`
	Objects   []objectRequest `json:"objects"`
}

type batchResponse struct {
	Transfer string           `json:"transfer"`
	Objects  []objectResponse `json:"objects"`
}

type responseError struct {
	Code             int    `json:"code,omitempty"`
	Message          string `json:"message,omitempty"`
	RequestID        string `json:"request_id,omitempty"`
	DocumentationURL string `json:"documentation_url,omitempty"`
}

func (b batchRequest) IsUpload() bool {
	return b.Operation == "upload"
}

func (b batchRequest) IsDownload() bool {
	return b.Operation == "download"
}

func newResponseError(ctx context.Context, status int, err error) (events.APIGatewayProxyResponse, error) {
	lc, _ := lambdacontext.FromContext(ctx)
	r := responseError{
		Message: err.Error(),
	}
	if lc != nil {
		r.RequestID = lc.AwsRequestID
	}

	b, _ := json.Marshal(r)

	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Body:       string(b),
	}, nil
}
