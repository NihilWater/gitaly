package unarycache

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb/testproto"
)

func TestRepoCacher_Lookup(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	cacher := NewRepoCacher("test")

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	request := &testproto.ValidRequest{
		Destination: repoProto,
	}

	t.Run("cache miss", func(t *testing.T) {
		_, err := cacher.Lookup(ctx, request)
		require.Equal(t, ErrCacheMiss, err)
	})
}

func TestRepoCacher_Write(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)

	cacher := NewRepoCacher("test")

	repoProto, _ := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	response := &testproto.ValidRequest{
		Destination: repoProto,
	}

	t.Run("write successfully", func(t *testing.T) {
		err := cacher.Write(ctx, response)
		require.NoError(t, err)
	})
}
