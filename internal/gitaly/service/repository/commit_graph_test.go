//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWriteCommitGraph_withExistingCommitGraphCreatedWithDefaults(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	commitGraphPath := filepath.Join(repoPath, stats.CommitGraphRelPath)
	require.NoError(t, os.RemoveAll(commitGraphPath))

	chainPath := filepath.Join(repoPath, stats.CommitGraphChainRelPath)
	require.NoFileExists(t, chainPath, "sanity check no commit graph chain exists")

	// write commit graph using an old approach
	gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable")
	require.FileExists(t, commitGraphPath)

	treeEntry := gittest.TreeEntry{Mode: "100644", Path: "file.txt", Content: "something"}
	gittest.WriteCommit(
		t,
		cfg,
		repoPath,
		gittest.WithBranch(t.Name()),
		gittest.WithTreeEntries(treeEntry),
	)

	//nolint:staticcheck
	res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)

	require.FileExists(t, chainPath, "commit graph chain should be created")
	requireBloomFilterUsed(t, repoPath)
	require.NoFileExists(t, commitGraphPath, "commit-graph file should be replaced with commit graph chain")
}

func TestWriteCommitGraph_withExistingCommitGraphCreatedWithSplit(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	commitGraphPath := filepath.Join(repoPath, stats.CommitGraphRelPath)
	require.NoError(t, os.RemoveAll(commitGraphPath))

	chainPath := filepath.Join(repoPath, stats.CommitGraphChainRelPath)
	require.NoFileExists(t, chainPath, "sanity check no commit graph chain exists")

	// write commit graph chain
	gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--reachable", "--split")
	require.FileExists(t, chainPath)

	treeEntry := gittest.TreeEntry{Mode: "100644", Path: "file.txt", Content: "something"}
	gittest.WriteCommit(
		t,
		cfg,
		repoPath,
		gittest.WithBranch(t.Name()),
		gittest.WithTreeEntries(treeEntry),
	)

	//nolint:staticcheck
	res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)

	require.FileExists(t, chainPath, "commit graph chain should be created")
	requireBloomFilterUsed(t, repoPath)
	require.NoFileExists(t, commitGraphPath, "commit-graph file should not be created")
}

func TestWriteCommitGraph(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	chainPath := filepath.Join(repoPath, stats.CommitGraphChainRelPath)

	require.NoFileExists(t, chainPath)

	//nolint:staticcheck
	res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)

	require.FileExists(t, chainPath)
}

func TestWriteCommitGraph_validationChecks(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, _, client := setupRepositoryService(t, ctx, testserver.WithDisablePraefect())

	for _, tc := range []struct {
		desc        string
		req         *gitalypb.WriteCommitGraphRequest
		expectedErr error
	}{
		{
			desc: "invalid split strategy",
			req: &gitalypb.WriteCommitGraphRequest{
				Repository:    repo,
				SplitStrategy: gitalypb.WriteCommitGraphRequest_SplitStrategy(42),
			},
			expectedErr: status.Error(codes.InvalidArgument, "unsupported split strategy: 42"),
		},
		{
			desc:        "no repository",
			req:         &gitalypb.WriteCommitGraphRequest{},
			expectedErr: status.Error(codes.InvalidArgument, "empty Repository"),
		},
		{
			desc:        "invalid storage",
			req:         &gitalypb.WriteCommitGraphRequest{Repository: &gitalypb.Repository{RelativePath: "stub", StorageName: "invalid"}},
			expectedErr: status.Error(codes.InvalidArgument, `getting commit-graph config: GetStorageByName: no such storage: "invalid"`),
		},
		{
			desc:        "not existing repository",
			req:         &gitalypb.WriteCommitGraphRequest{Repository: &gitalypb.Repository{StorageName: repo.StorageName, RelativePath: "invalid"}},
			expectedErr: status.Error(codes.NotFound, fmt.Sprintf(`getting commit-graph config: GetRepoPath: not a git repository: "%s/invalid"`, cfg.Storages[0].Path)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			//nolint:staticcheck
			_, err := client.WriteCommitGraph(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)
		})
	}
}

func TestUpdateCommitGraph(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(t, ctx)

	chainPath := filepath.Join(repoPath, stats.CommitGraphChainRelPath)
	require.NoFileExists(t, chainPath)

	//nolint:staticcheck
	res, err := client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.FileExists(t, chainPath)

	// Reset the mtime of commit-graph-chain file to use
	// as basis to detect changes
	require.NoError(t, os.Chtimes(chainPath, time.Time{}, time.Time{}))
	info, err := os.Stat(chainPath)
	require.NoError(t, err)
	mt := info.ModTime()

	treeEntry := gittest.TreeEntry{Mode: "100644", Path: "file.txt", Content: "something"}
	gittest.WriteCommit(
		t,
		cfg,
		repoPath,
		gittest.WithBranch(t.Name()),
		gittest.WithTreeEntries(treeEntry),
	)

	//nolint:staticcheck
	res, err = client.WriteCommitGraph(ctx, &gitalypb.WriteCommitGraphRequest{
		Repository:    repo,
		SplitStrategy: gitalypb.WriteCommitGraphRequest_SizeMultiple,
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.FileExists(t, chainPath)

	assertModTimeAfter(t, mt, chainPath)
}

func requireBloomFilterUsed(tb testing.TB, repoPath string) {
	tb.Helper()

	commitGraphsPath := filepath.Join(repoPath, stats.CommitGraphChainRelPath)
	ids := bytes.Split(testhelper.MustReadFile(tb, commitGraphsPath), []byte{'\n'})

	for _, id := range ids {
		if len(id) == 0 {
			continue
		}
		graphFilePath := filepath.Join(repoPath, filepath.Dir(stats.CommitGraphChainRelPath), fmt.Sprintf("graph-%s.graph", id))
		graphFileData := testhelper.MustReadFile(tb, graphFilePath)

		require.True(tb, bytes.HasPrefix(graphFileData, []byte("CGPH")), "4-byte signature of the commit graph file")
		require.True(tb, bytes.Contains(graphFileData, []byte("BIDX")), "Bloom Filter Index")
		require.True(tb, bytes.Contains(graphFileData, []byte("BDAT")), "Bloom Filter Data")
	}
}
