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
    "fmt"

	"github.com/aws/smithy-go/middleware"
    "github.com/aws/smithy-go/transport/http"
    "github.com/aws/smithy-go"
	"k8s.io/klog/v2"
)

// Handler for aws-sdk-go-v2 that logs all requests
type awsHandlerLogger struct{}
func (l *awsHandlerLogger) ID() string {
    return "k8s/logger"
}
func (l *awsHandlerLogger) HandleFinalize(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
    out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
) {
    service, name := awsServiceAndName(ctx)
    klog.V(4).Infof("AWS request: %s %s", service, name)
    return next.HandleFinalize(ctx, in)
}

type awsValidateResponseHandlerLogger struct{}
func (l *awsValidateResponseHandlerLogger) ID() string {
    return "k8s/api-validate-response"
}
func (l *awsValidateResponseHandlerLogger) HandleDeserialize(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (
    out middleware.DeserializeOutput, metadata middleware.Metadata, err error,
) {
    out, metadata, err = next.HandleDeserialize(ctx, in)
    response, ok := out.RawResponse.(*http.Response)
	if !ok {
		return out, metadata, &smithy.DeserializationError{Err: fmt.Errorf("unknown transport type %T", out.RawResponse)}
	}
    service, name := awsServiceAndName(ctx)
	klog.V(4).Infof("AWS API ValidateResponse: %s %s %d", service, name, response.StatusCode)
    return out, metadata, err
}

func awsServiceAndName(ctx context.Context) (string, string) {
    service := middleware.GetServiceID(ctx)

    name := "?"
    if opName := middleware.GetOperationName(ctx); opName != "" {
        name = opName
    }
    return service, name
}