package unarycache

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// RepoCacher is a Cacher that caches responses that are bound to a single
// repository. This cache is written inside the repository.
type RepoCacher struct {
	base string
}

// NewRepoCacher creates a new instance of a unary RepoCacher.
// It takes a base, which is the base for the filename on disk.
func NewRepoCacher(base string) RepoCacher {
	return RepoCacher{
		base: base,
	}
}

// Lookup the response in the cache and return it if found, or return
// ErrCacheMiss otherwise.
func (c *RepoCacher) Lookup(ctx context.Context, req proto.Message) (proto.Message, error) {
	return nil, ErrCacheMiss
}

// Write the response to the cache.
func (c *RepoCacher) Write(ctx context.Context, resp proto.Message) error {
	return nil
}
