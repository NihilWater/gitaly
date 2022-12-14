//go:build !gitaly_test_sha256

// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package proxy_test

import (
	"context"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpc_metadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var director proxy.StreamDirector

func ExampleRegisterService() {
	// A gRPC server with the proxying codec enabled.
	server := grpc.NewServer(grpc.ForceServerCodec(proxy.NewCodec()))
	// Register a TestService with 4 of its methods explicitly.
	proxy.RegisterService(server, director,
		"mwitkow.testproto.TestService",
		"PingEmpty", "Ping", "PingError", "PingList")
}

func ExampleTransparentHandler() {
	grpc.NewServer(
		grpc.ForceServerCodec(proxy.NewCodec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)))
}

// Provide sa simple example of a director that shields internal services and dials a staging or production backend.
// This is a *very naive* implementation that creates a new connection on every request. Consider using pooling.
func ExampleStreamDirector() {
	director = func(ctx context.Context, fullMethodName string, _ proxy.StreamPeeker) (*proxy.StreamParameters, error) {
		// Make sure we never forward internal services.
		if strings.HasPrefix(fullMethodName, "/com.example.internal.") {
			return nil, status.Errorf(codes.Unimplemented, "Unknown method")
		}
		md, ok := grpc_metadata.FromIncomingContext(ctx)
		if ok {
			// Decide on which backend to dial
			if val, exists := md[":authority"]; exists && val[0] == "staging.api.example.com" {
				// Make sure we use DialContext so the dialing can be cancelled/time out together with the context.
				conn, err := grpc.DialContext(ctx, "api-service.staging.svc.local", grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())))
				return proxy.NewStreamParameters(proxy.Destination{
					Conn: conn,
					Ctx:  metadata.IncomingToOutgoing(ctx),
				}, nil, nil, nil), err
			} else if val, exists := md[":authority"]; exists && val[0] == "api.example.com" {
				conn, err := grpc.DialContext(ctx, "api-service.prod.svc.local", grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())))
				return proxy.NewStreamParameters(proxy.Destination{
					Conn: conn,
					Ctx:  metadata.IncomingToOutgoing(ctx),
				}, nil, nil, nil), err
			}
		}
		return nil, status.Errorf(codes.Unimplemented, "Unknown method")
	}
}
