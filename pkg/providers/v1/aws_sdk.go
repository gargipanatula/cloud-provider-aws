/*
Copyright 2024 The Kubernetes Authors.

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
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	elb "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	smithymiddleware "github.com/aws/smithy-go/middleware"

	"k8s.io/client-go/pkg/version"
	"k8s.io/klog/v2"
	"k8s.io/cloud-provider-aws/pkg/providers/v1/config"
	"k8s.io/cloud-provider-aws/pkg/providers/v1/iface"
)

type awsSDKProvider struct {
	creds aws.CredentialsProvider
	cfg   awsCloudConfigProvider

	mutex          sync.Mutex
	regionDelayers map[string]*CrossRequestRetryDelay
}

func newAWSSDKProvider(creds aws.CredentialsProvider, cfg *config.CloudConfig) *awsSDKProvider {
	return &awsSDKProvider{
		creds:          creds,
		cfg:            cfg,
		regionDelayers: make(map[string]*CrossRequestRetryDelay),
	}
}

func (p *awsSDKProvider) AddHandlers(ctx context.Context, regionName string, cfg aws.Config) {
	// TODO: add retryer
	cfg.APIOptions = append(cfg.APIOptions,
		middleware.AddUserAgentKeyValue("kubernetes", version.Get().String()),
		func(stack *smithymiddleware.Stack) error {
			return stack.Finalize.Add(&awsHandlerLogger{}, smithymiddleware.Before)
		},
	)

	delayer := p.getCrossRequestRetryDelay(regionName)
	if delayer != nil {
		cfg.APIOptions = append(cfg.APIOptions,
			func(stack *smithymiddleware.Stack) error {
				return stack.Finalize.Add(&delayPrerequest{
					delayer: delayer,
				}, smithymiddleware.Before)
			},
			func(stack *smithymiddleware.Stack) error {
				return stack.Finalize.Insert(&delayAfterRetry{
					delayer: delayer,
				}, "Retry", smithymiddleware.Before)
			},
		)
	}

	p.addAPILoggingHandlers(cfg)
}

func (p *awsSDKProvider) addAPILoggingHandlers(cfg aws.Config) {
	cfg.ClientLogMode = aws.LogRequest

	cfg.APIOptions = append(cfg.APIOptions,
		middleware.AddUserAgentKeyValue("kubernetes", version.Get().String()),
		func(stack *smithymiddleware.Stack) error {
			return stack.Deserialize.Add(&awsValidateResponseHandlerLogger{}, smithymiddleware.Before)
		},
	)
}

// Get a CrossRequestRetryDelay, scoped to the region, not to the request.
// This means that when we hit a limit on a call, we will delay _all_ calls to the API.
// We do this to protect the AWS account from becoming overloaded and effectively locked.
// We also log when we hit request limits.
// Note that this delays the current goroutine; this is bad behaviour and will
// likely cause k8s to become slow or unresponsive for cloud operations.
// However, this throttle is intended only as a last resort.  When we observe
// this throttling, we need to address the root cause (e.g. add a delay to a
// controller retry loop)
func (p *awsSDKProvider) getCrossRequestRetryDelay(regionName string) *CrossRequestRetryDelay {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	delayer, found := p.regionDelayers[regionName]
	if !found {
		delayer = NewCrossRequestRetryDelay()
		p.regionDelayers[regionName] = delayer
	}
	return delayer
}

func (p *awsSDKProvider) Compute(ctx context.Context, regionName string) (iface.EC2, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO(),
		awsConfig.WithRegion(regionName),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize AWS config: %v", err)
	}
	cfg.Region = regionName
	cfg.Credentials = p.creds

	p.AddHandlers(ctx, regionName, cfg)

	ec2Client := ec2.NewFromConfig(cfg, func(o *ec2.Options) {
		// o.EndpointResolverV2 = "temp",
		o.Retryer = &customRetryer{
			retry.NewStandard(),
		}
	})

	ec2 := &awsSdkEC2{
		ec2: ec2Client,
	}
	return ec2, nil
}

func (p *awsSDKProvider) LoadBalancing(ctx context.Context, regionName string) (ELB, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO(),
		awsConfig.WithRegion(regionName),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize AWS config: %v", err)
	}
	cfg.Region = regionName
	cfg.Credentials = p.creds

	p.AddHandlers(ctx, regionName, cfg)

	elbClient := elb.NewFromConfig(cfg)
	return elbClient, nil
}

func (p *awsSDKProvider) LoadBalancingV2(ctx context.Context, regionName string) (ELBV2, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO(),
		awsConfig.WithRegion(regionName),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize AWS config: %v", err)
	}
	cfg.Region = regionName
	cfg.Credentials = p.creds

	p.AddHandlers(ctx, regionName, cfg)

	elbv2Client := elbv2.NewFromConfig(cfg)
	return elbv2Client, nil
}

func (p *awsSDKProvider) Metadata() (config.EC2Metadata, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("unable to initialize AWS config: %v", err)
	}
	p.addAPILoggingHandlers(cfg)
	imdsClient := imds.New(imds.Options{ClientEnableState: imds.ClientEnabled})
	getInstanceIdentityDocumentOutput, err := imdsClient.GetInstanceIdentityDocument(context.Background(), &imds.GetInstanceIdentityDocumentInput{})
	identity := getInstanceIdentityDocumentOutput.InstanceIdentityDocument
	// identity, err := client.GetInstanceIdentityDocument()
	if err == nil {
		klog.InfoS("instance metadata identity",
			"region", identity.Region,
			"availability-zone", identity.AvailabilityZone,
			"instance-type", identity.InstanceType,
			"architecture", identity.Architecture,
			"instance-id", identity.InstanceID,
			"private-ip", identity.PrivateIP,
			"account-id", identity.AccountID,
			"image-id", identity.ImageID)
	}
	return imdsClient, nil
}

func (p *awsSDKProvider) KeyManagement(ctx context.Context, regionName string) (KMS, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO(),
		awsConfig.WithRegion(regionName),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize AWS config: %v", err)
	}
	cfg.Credentials = p.creds

	p.AddHandlers(ctx, regionName, cfg)

	kmsClient := kms.NewFromConfig(cfg)

	return kmsClient, nil
}
