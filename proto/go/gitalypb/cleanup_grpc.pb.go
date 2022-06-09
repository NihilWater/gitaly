// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.17.3
// source: cleanup.proto

package gitalypb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// CleanupServiceClient is the client API for CleanupService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type CleanupServiceClient interface {
	// This comment is left unintentionally blank.
	ApplyBfgObjectMapStream(ctx context.Context, opts ...grpc.CallOption) (CleanupService_ApplyBfgObjectMapStreamClient, error)
}

type cleanupServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewCleanupServiceClient(cc grpc.ClientConnInterface) CleanupServiceClient {
	return &cleanupServiceClient{cc}
}

func (c *cleanupServiceClient) ApplyBfgObjectMapStream(ctx context.Context, opts ...grpc.CallOption) (CleanupService_ApplyBfgObjectMapStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &CleanupService_ServiceDesc.Streams[0], "/gitaly.CleanupService/ApplyBfgObjectMapStream", opts...)
	if err != nil {
		return nil, err
	}
	x := &cleanupServiceApplyBfgObjectMapStreamClient{stream}
	return x, nil
}

type CleanupService_ApplyBfgObjectMapStreamClient interface {
	Send(*ApplyBfgObjectMapStreamRequest) error
	Recv() (*ApplyBfgObjectMapStreamResponse, error)
	grpc.ClientStream
}

type cleanupServiceApplyBfgObjectMapStreamClient struct {
	grpc.ClientStream
}

func (x *cleanupServiceApplyBfgObjectMapStreamClient) Send(m *ApplyBfgObjectMapStreamRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *cleanupServiceApplyBfgObjectMapStreamClient) Recv() (*ApplyBfgObjectMapStreamResponse, error) {
	m := new(ApplyBfgObjectMapStreamResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// CleanupServiceServer is the server API for CleanupService service.
// All implementations must embed UnimplementedCleanupServiceServer
// for forward compatibility
type CleanupServiceServer interface {
	// This comment is left unintentionally blank.
	ApplyBfgObjectMapStream(CleanupService_ApplyBfgObjectMapStreamServer) error
	mustEmbedUnimplementedCleanupServiceServer()
}

// UnimplementedCleanupServiceServer must be embedded to have forward compatible implementations.
type UnimplementedCleanupServiceServer struct {
}

func (UnimplementedCleanupServiceServer) ApplyBfgObjectMapStream(CleanupService_ApplyBfgObjectMapStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method ApplyBfgObjectMapStream not implemented")
}
func (UnimplementedCleanupServiceServer) mustEmbedUnimplementedCleanupServiceServer() {}

// UnsafeCleanupServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to CleanupServiceServer will
// result in compilation errors.
type UnsafeCleanupServiceServer interface {
	mustEmbedUnimplementedCleanupServiceServer()
}

func RegisterCleanupServiceServer(s grpc.ServiceRegistrar, srv CleanupServiceServer) {
	s.RegisterService(&CleanupService_ServiceDesc, srv)
}

func _CleanupService_ApplyBfgObjectMapStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(CleanupServiceServer).ApplyBfgObjectMapStream(&cleanupServiceApplyBfgObjectMapStreamServer{stream})
}

type CleanupService_ApplyBfgObjectMapStreamServer interface {
	Send(*ApplyBfgObjectMapStreamResponse) error
	Recv() (*ApplyBfgObjectMapStreamRequest, error)
	grpc.ServerStream
}

type cleanupServiceApplyBfgObjectMapStreamServer struct {
	grpc.ServerStream
}

func (x *cleanupServiceApplyBfgObjectMapStreamServer) Send(m *ApplyBfgObjectMapStreamResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *cleanupServiceApplyBfgObjectMapStreamServer) Recv() (*ApplyBfgObjectMapStreamRequest, error) {
	m := new(ApplyBfgObjectMapStreamRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// CleanupService_ServiceDesc is the grpc.ServiceDesc for CleanupService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var CleanupService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gitaly.CleanupService",
	HandlerType: (*CleanupServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ApplyBfgObjectMapStream",
			Handler:       _CleanupService_ApplyBfgObjectMapStream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "cleanup.proto",
}
