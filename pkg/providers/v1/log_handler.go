/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aws

import (
	"context"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/smithy-go/middleware"
	"k8s.io/klog/v2"
)


type awsLogger struct{}

func (l *awsLogger) ID() string {
    return "k8s/logger"
}

func (l *awsLogger) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
    out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
) {
    awsHandlerLogger(ctx, in)
    return next.HandleFinalize(ctx, in)
}

// Handler for aws-sdk-go-v2 that logs all requests
func awsHandlerLogger(ctx context.Context, in middleware.FinalizeInput) {
    service, name := awsServiceAndName(ctx, in)
    klog.V(4).Infof("AWS request: %s %s", service, name)
}

func awsSendHandlerLogger(req *request.Request) {
	service, name := awsServiceAndName(req)
	klog.V(4).Infof("AWS API Send: %s %s %v %v", service, name, req.Operation, req.Params)
}

func awsValidateResponseHandlerLogger(req *request.Request) {
	service, name := awsServiceAndName(req)
	klog.V(4).Infof("AWS API ValidateResponse: %s %s %v %v %s", service, name, req.Operation, req.Params, req.HTTPResponse.Status)
}

func awsServiceAndName(ctx context.Context, in middleware.FinalizeInput) (string, string) {
    service := middleware.GetServiceID(ctx)

    name := "?"
    if opName := middleware.GetOperationName(ctx); opName != "" {
        name = opName
    }
    return service, name
}
