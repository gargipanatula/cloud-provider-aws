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

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	stscredsv2 "github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	elb "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/kms"

	smithymiddleware "github.com/aws/smithy-go/middleware"

	"k8s.io/client-go/pkg/version"
	"k8s.io/cloud-provider-aws/pkg/providers/v1/config"
	"k8s.io/cloud-provider-aws/pkg/providers/v1/iface"
	"k8s.io/klog/v2"
)

type awsSDKProvider struct {
	creds   *credentials.Credentials
	credsV2 awsv2.CredentialsProvider // for use in aws-sdk-go v2 clients
	cfg     awsCloudConfigProvider

	mutex          sync.Mutex
	regionDelayers map[string]*CrossRequestRetryDelay
}

func newAWSSDKProvider(creds *credentials.Credentials, credsV2 awsv2.CredentialsProvider, cfg *config.CloudConfig) *awsSDKProvider {
	return &awsSDKProvider{
		creds:          creds,
		credsV2:        credsV2,
		cfg:            cfg,
		regionDelayers: make(map[string]*CrossRequestRetryDelay),
	}
}

func (p *awsSDKProvider) AddHandlers(regionName string, h *request.Handlers) {
	h.Build.PushFrontNamed(request.NamedHandler{
		Name: "k8s/user-agent",
		Fn:   request.MakeAddToUserAgentHandler("kubernetes", version.Get().String()),
	})

	h.Sign.PushFrontNamed(request.NamedHandler{
		Name: "k8s/logger",
		Fn:   awsHandlerLogger,
	})

	delayer := p.getCrossRequestRetryDelay(regionName)
	if delayer != nil {
		h.Sign.PushFrontNamed(request.NamedHandler{
			Name: "k8s/delay-presign",
			Fn:   delayer.BeforeSign,
		})

		h.AfterRetry.PushFrontNamed(request.NamedHandler{
			Name: "k8s/delay-afterretry",
			Fn:   delayer.AfterRetry,
		})
	}

	p.addAPILoggingHandlers(h)
}

func (p *awsSDKProvider) addAPILoggingHandlers(h *request.Handlers) {
	h.Send.PushBackNamed(request.NamedHandler{
		Name: "k8s/api-request",
		Fn:   awsSendHandlerLogger,
	})

	h.ValidateResponse.PushFrontNamed(request.NamedHandler{
		Name: "k8s/api-validate-response",
		Fn:   awsValidateResponseHandlerLogger,
	})
}

// Adds handlers to AWS SDK Go V2 clients. For AWS SDK Go V1 clients,
// func (p *awsSDKProvider) AddHandlers is used.
func (p *awsSDKProvider) AddHandlersV2(ctx context.Context, regionName string, cfg *awsv2.Config) {
	cfg.APIOptions = append(cfg.APIOptions,
		middleware.AddUserAgentKeyValue("kubernetes", version.Get().String()),
		func(stack *smithymiddleware.Stack) error {
			return stack.Finalize.Add(awsHandlerLoggerMiddleware(), smithymiddleware.Before)
		},
	)

	delayer := p.getCrossRequestRetryDelay(regionName)
	if delayer != nil {
		cfg.APIOptions = append(cfg.APIOptions,
			func(stack *smithymiddleware.Stack) error {
				stack.Finalize.Add(delayPreSign(delayer), smithymiddleware.Before)
				stack.Finalize.Insert(delayAfterRetry(delayer), "Retry", smithymiddleware.Before)
				return nil
			},
		)
	}

	p.addAPILoggingHandlersV2(cfg)
}

// Adds logging middleware for AWS SDK Go V2 clients
func (p *awsSDKProvider) addAPILoggingHandlersV2(cfg *awsv2.Config) {
	cfg.APIOptions = append(cfg.APIOptions,
		func(stack *smithymiddleware.Stack) error {
			stack.Serialize.Add(awsSendHandlerLoggerMiddleware(), smithymiddleware.After)
			stack.Deserialize.Add(awsValidateResponseHandlerLoggerMiddleware(), smithymiddleware.Before)
			return nil
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

func (p *awsSDKProvider) Compute(ctx context.Context, regionName string, assumeRoleProvider *stscredsv2.AssumeRoleProvider) (iface.EC2, error) {
	cfg, err := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithDefaultsMode(awsv2.DefaultsModeInRegion),
		awsConfig.WithRegion(regionName),
	)
	if assumeRoleProvider != nil {
		cfg.Credentials = awsv2.NewCredentialsCache(assumeRoleProvider)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to initialize AWS config: %v", err)
	}

	p.AddHandlersV2(ctx, regionName, &cfg)
	var opts []func(*ec2.Options) = p.cfg.GetEC2EndpointOpts(regionName)
	opts = append(opts, func(o *ec2.Options) {
		o.Retryer = &customRetryer{
			retry.NewStandard(),
		}
		o.EndpointResolverV2 = p.cfg.GetCustomEC2Resolver()
	})

	ec2Client := ec2.NewFromConfig(cfg, opts...)

	ec2 := &awsSdkEC2{
		ec2: ec2Client,
	}
	return ec2, nil
}

func (p *awsSDKProvider) LoadBalancing(ctx context.Context, regionName string, assumeRoleProvider *stscredsv2.AssumeRoleProvider) (ELB, error) {
	cfg, err := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithDefaultsMode(awsv2.DefaultsModeInRegion),
		awsConfig.WithRegion(regionName),
	)
	if assumeRoleProvider != nil {
		cfg.Credentials = awsv2.NewCredentialsCache(assumeRoleProvider)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to initialize AWS config: %v", err)
	}

	p.AddHandlersV2(ctx, regionName, &cfg)
	var opts []func(*elb.Options) = p.cfg.GetELBEndpointOpts(regionName)
	opts = append(opts, func(o *elb.Options) {
		o.Retryer = &customRetryer{
			retry.NewStandard(),
		}
		o.EndpointResolverV2 = p.cfg.GetCustomELBResolver()
	})

	elbClient := elb.NewFromConfig(cfg, opts...)

	return elbClient, nil
}

func (p *awsSDKProvider) LoadBalancingV2(ctx context.Context, regionName string, assumeRoleProvider *stscredsv2.AssumeRoleProvider) (ELBV2, error) {
	cfg, err := awsConfig.LoadDefaultConfig(ctx, awsConfig.WithDefaultsMode(awsv2.DefaultsModeInRegion),
		awsConfig.WithRegion(regionName),
	)
	if assumeRoleProvider != nil {
		cfg.Credentials = awsv2.NewCredentialsCache(assumeRoleProvider)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to initialize AWS config: %v", err)
	}

	p.AddHandlersV2(ctx, regionName, &cfg)
	var opts []func(*elbv2.Options) = p.cfg.GetELBV2EndpointOpts(regionName)
	opts = append(opts, func(o *elbv2.Options) {
		o.Retryer = &customRetryer{
			retry.NewStandard(),
		}
		o.EndpointResolverV2 = p.cfg.GetCustomELBV2Resolver()
	})

	elbv2Client := elbv2.NewFromConfig(cfg, opts...)

	return elbv2Client, nil
}

func (p *awsSDKProvider) Metadata() (config.EC2Metadata, error) {
	sess, err := session.NewSession(&aws.Config{
		EndpointResolver: p.cfg.GetResolver(),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to initialize AWS session: %v", err)
	}
	client := ec2metadata.New(sess)
	p.addAPILoggingHandlers(&client.Handlers)

	identity, err := client.GetInstanceIdentityDocument()
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
	return client, nil
}

func (p *awsSDKProvider) KeyManagement(regionName string) (KMS, error) {
	awsConfig := &aws.Config{
		Region:      &regionName,
		Credentials: p.creds,
	}
	awsConfig = awsConfig.WithCredentialsChainVerboseErrors(true).
		WithEndpointResolver(p.cfg.GetResolver())
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:            *awsConfig,
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to initialize AWS session: %v", err)
	}
	kmsClient := kms.New(sess)

	p.AddHandlers(regionName, &kmsClient.Handlers)

	return kmsClient, nil
}
