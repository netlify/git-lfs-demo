package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	switch request.Path {
	case "/.netlify/functions/lfs":
		return events.APIGatewayProxyResponse{
			StatusCode: 200,
			Body:       "Netlify LFS server",
		}, nil
	case "/.netlify/functions/lfs/objects/batch":
		return objectsCommand(ctx, request)
	case "/.netlify/functions/lfs/verify":
		return verifyCommand(ctx, request)
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 404,
	}, nil
}

func objectsCommand(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	if request.HTTPMethod == "GET" {
		return signDownloads(ctx, request)
	}

	if request.HTTPMethod == "POST" {
		return signUploads(ctx, request)
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 404,
	}, nil
}

func signUploads(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var b batchRequest
	if err := json.NewDecoder(strings.NewReader(request.Body)).Decode(&b); err != nil {
		return newResponseError(ctx, 422, err)
	}

	if !b.IsUpload() {
		return newResponseError(ctx, 422, errors.New("not an upload batch object"))
	}

	if len(b.Objects) == 0 {
		return events.APIGatewayProxyResponse{
			StatusCode: 200,
			Body:       "{}",
			Headers: map[string]string{
				"Content-Type": "application/vnd.git-lfs+json",
			},
		}, nil
	}

	var transfer string
	if len(b.Transfers) > 0 {
		transfer = b.Transfers[0]
	}
	return signObjectUploads(ctx, transfer, b.Objects)
}

func signDownloads(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var b batchRequest
	if err := json.NewDecoder(strings.NewReader(request.Body)).Decode(&b); err != nil {
		return newResponseError(ctx, 422, err)
	}

	if !b.IsDownload() {
		return newResponseError(ctx, 422, errors.New("not a download batch object"))
	}

	if len(b.Objects) == 0 {
		return events.APIGatewayProxyResponse{
			StatusCode: 200,
			Body:       "{}",
			Headers: map[string]string{
				"Content-Type": "application/vnd.git-lfs+json",
			},
		}, nil
	}

	return signObjectDownloads(ctx, b.Transfers[0], b.Objects)
}

func verifyCommand(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var o objectRequest
	if err := json.NewDecoder(strings.NewReader(request.Body)).Decode(&o); err != nil {
		return newResponseError(ctx, 422, err)
	}

	return verifyObject(ctx, o)
}

func main() {
	// Make the handler available for Remote Procedure Call by AWS Lambda
	lambda.Start(handler)
}
