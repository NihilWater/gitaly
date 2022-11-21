// Package unarycache allows you to cache responses for unary gRPC messages.
//
// Some gRPC unary message take a considerable amount of compute to build the
// response. This package will allow users to cache this response. The package
// is designed so it can easily wrap around any ordinary unary gRPC message
// handler, but cache the response when doing so.
package unarycache

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ErrCacheMiss is an error used when the entry could be found in the cache.
var ErrCacheMiss = errors.New("cache miss")

// Cacher is the interface which cache components should implement.
type Cacher interface {
	// Lookup the response in the cache and return it if found, or return
	// ErrCacheMiss otherwise.
	Lookup(ctx context.Context, req proto.Message) (proto.Message, error)
	// Write the response to the cache.
	Write(ctx context.Context, resp proto.Message) error
}

// WithCache asks the Cacher to lookup the response in the cache and return it.
// If it's not found it will create the response, write it in the Cacher, and
// return it.
func WithCache[Q proto.Message, A proto.Message](ctx context.Context, req Q, cacher Cacher, create func(context.Context, Q) (A, error)) (A, error) {
	var response A

	resp, err := cacher.Lookup(ctx, req)
	if err == nil {
		response, ok := resp.(A)
		if ok {
			return response, nil
		}
		return response, fmt.Errorf("with cache: type assertion")
	}
	if !errors.Is(err, ErrCacheMiss) {
		return response, fmt.Errorf("with cache: %w", err)
	}

	response, err = create(ctx, req)
	if err != nil {
		return response, fmt.Errorf("with cache: %w", err)
	}

	if err := cacher.Write(ctx, response); err != nil {
		return response, fmt.Errorf("with cache: %w", err)
	}

	return response, nil
}
