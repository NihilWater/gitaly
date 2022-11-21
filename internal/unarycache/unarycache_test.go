package unarycache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb/testproto"
)

func TestWithCache(t *testing.T) {
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

	t.Run("returns identical response", func(t *testing.T) {
		method := func(context.Context, *testproto.ValidRequest) (*testproto.ValidStorageRequest, error) {
			return &testproto.ValidStorageRequest{StorageName: "hello"}, nil
		}

		expected, err := method(ctx, request)
		require.NoError(t, err)

		actual, err := WithCache(ctx, request, &cacher, method)
		require.NoError(t, err)
		require.Equal(t, "hello", actual.GetStorageName())

		testhelper.ProtoEqual(t, expected, actual)
	})
}
