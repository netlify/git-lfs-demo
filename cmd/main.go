package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	parts := strings.Split(request.Path, "/")

	switch parts[0] {
	case "":
		return events.APIGatewayProxyResponse{
			StatusCode: 200,
			Body:       "Netlify LFS server",
		}, nil
	case "objects":
		return objectsCommand(ctx, request, parts[1:])
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 404,
	}, nil
}

func objectsCommand(ctx context.Context, request events.APIGatewayProxyRequest, parts []string) (events.APIGatewayProxyResponse, error) {
	if request.HTTPMethod == "GET" {
		return signDownloads(ctx, request)
	}

	if request.HTTPMethod == "POST" && len(parts) == 0 {
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
		}, nil
	}

	return signObjectUploads(ctx, b.Transfers[0], b.Objects)
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
		}, nil
	}

	return signObjectDownloads(ctx, b.Transfers[0], b.Objects)
}

func main() {
	// Make the handler available for Remote Procedure Call by AWS Lambda
	lambda.Start(handler)
}
