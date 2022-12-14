//go:build !gitaly_test_sha256

package repository

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestApplyGitattributesSuccess(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	_, repo, repoPath, client := setupRepositoryService(t, ctx)

	infoPath := filepath.Join(repoPath, "info")
	attributesPath := filepath.Join(infoPath, "attributes")

	tests := []struct {
		desc     string
		revision []byte
		contents []byte
	}{
		{
			desc:     "With a .gitattributes file",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			contents: []byte("/custom-highlighting/*.gitlab-custom gitlab-language=ruby\n"),
		},
		{
			desc:     "Without a .gitattributes file",
			revision: []byte("7efb185dd22fd5c51ef044795d62b7847900c341"),
			contents: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Test when no /info folder exists
			if err := os.RemoveAll(infoPath); err != nil {
				t.Fatal(err)
			}
			assertGitattributesApplied(t, ctx, client, repo, attributesPath, test.revision, test.contents)

			// Test when no git attributes file exists
			if err := os.Remove(attributesPath); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			assertGitattributesApplied(t, ctx, client, repo, attributesPath, test.revision, test.contents)

			// Test when a git attributes file already exists
			require.NoError(t, os.MkdirAll(infoPath, 0o755))
			require.NoError(t, os.WriteFile(attributesPath, []byte("*.docx diff=word"), 0o644))
			assertGitattributesApplied(t, ctx, client, repo, attributesPath, test.revision, test.contents)
		})
	}
}

type testTransactionServer struct {
	gitalypb.UnimplementedRefTransactionServer
	vote func(*gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error)
}

func (s *testTransactionServer) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	if s.vote != nil {
		return s.vote(in)
	}
	return nil, nil
}

func TestApplyGitattributesWithTransaction(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	transactionServer := &testTransactionServer{}
	runRepositoryService(t, cfg, nil)

	// We're using internal listener in order to route around
	// Praefect in our tests. Otherwise Praefect would replace our
	// carefully crafted transaction and server information.
	logger := testhelper.NewDiscardingLogEntry(t)

	client := newMuxedRepositoryClient(t, ctx, cfg, "unix://"+cfg.InternalSocketPath(),
		backchannel.NewClientHandshaker(logger, func() backchannel.Server {
			srv := grpc.NewServer()
			gitalypb.RegisterRefTransactionServer(srv, transactionServer)
			return srv
		}),
	)

	for _, tc := range []struct {
		desc          string
		revision      []byte
		voteFn        func(*testing.T, *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error)
		shouldExist   bool
		expectedErr   error
		expectedVotes int
	}{
		{
			desc:     "successful vote writes gitattributes",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			voteFn: func(t *testing.T, request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				vote := voting.VoteFromData([]byte("/custom-highlighting/*.gitlab-custom gitlab-language=ruby\n"))
				expectedHash := vote.Bytes()

				require.Equal(t, expectedHash, request.ReferenceUpdatesHash)
				return &gitalypb.VoteTransactionResponse{
					State: gitalypb.VoteTransactionResponse_COMMIT,
				}, nil
			},
			shouldExist:   true,
			expectedVotes: 2,
		},
		{
			desc:     "aborted vote does not write gitattributes",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			voteFn: func(t *testing.T, request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				return &gitalypb.VoteTransactionResponse{
					State: gitalypb.VoteTransactionResponse_ABORT,
				}, nil
			},
			shouldExist: false,
			expectedErr: func() error {
				return status.Error(codes.Internal, "committing gitattributes: voting on locked file: preimage vote: transaction was aborted")
			}(),
			expectedVotes: 1,
		},
		{
			desc:     "failing vote does not write gitattributes",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			voteFn: func(t *testing.T, request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				return nil, errors.New("foobar")
			},
			shouldExist: false,
			expectedErr: func() error {
				return status.Error(codes.Internal, "committing gitattributes: voting on locked file: preimage vote: rpc error: code = Unknown desc = foobar")
			}(),
			expectedVotes: 1,
		},
		{
			desc:     "commit without gitattributes performs vote",
			revision: []byte("7efb185dd22fd5c51ef044795d62b7847900c341"),
			voteFn: func(t *testing.T, request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				require.Equal(t, bytes.Repeat([]byte{0x00}, 20), request.ReferenceUpdatesHash)
				return &gitalypb.VoteTransactionResponse{
					State: gitalypb.VoteTransactionResponse_COMMIT,
				}, nil
			},
			shouldExist:   false,
			expectedVotes: 2,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			infoPath := filepath.Join(repoPath, "info")
			require.NoError(t, os.RemoveAll(infoPath))

			ctx, err := txinfo.InjectTransaction(ctx, 1, "primary", true)
			require.NoError(t, err)
			ctx = metadata.IncomingToOutgoing(ctx)

			var votes int
			transactionServer.vote = func(request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				votes++
				return tc.voteFn(t, request)
			}

			_, err = client.ApplyGitattributes(ctx, &gitalypb.ApplyGitattributesRequest{
				Repository: repo,
				Revision:   tc.revision,
			})
			testhelper.RequireGrpcError(t, tc.expectedErr, err)

			path := filepath.Join(infoPath, "attributes")
			if tc.shouldExist {
				require.FileExists(t, path)
				contents := testhelper.MustReadFile(t, path)
				require.Equal(t, []byte("/custom-highlighting/*.gitlab-custom gitlab-language=ruby\n"), contents)
			} else {
				require.NoFileExists(t, path)
			}
			require.Equal(t, tc.expectedVotes, votes)
		})
	}
}

func TestApplyGitattributesFailure(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg, repo, _, client := setupRepositoryService(t, ctx)

	tests := []struct {
		desc        string
		repo        *gitalypb.Repository
		revision    []byte
		expectedErr error
	}{
		{
			desc:     "no repository provided",
			repo:     nil,
			revision: nil,
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefectMessage(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc:     "unknown storage provided",
			repo:     &gitalypb.Repository{RelativePath: "stub", StorageName: "foo"},
			revision: []byte("master"),
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefectMessage(
				`GetStorageByName: no such storage: "foo"`,
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc:     "storage not provided",
			repo:     &gitalypb.Repository{RelativePath: repo.GetRelativePath()},
			revision: []byte("master"),
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefectMessage(
				"empty StorageName",
				"repo scoped: invalid Repository",
			)),
		},
		{
			desc:     "repository doesn't exist on disk",
			repo:     &gitalypb.Repository{StorageName: repo.GetStorageName(), RelativePath: "bar"},
			revision: []byte("master"),
			expectedErr: status.Error(codes.NotFound, testhelper.GitalyOrPraefectMessage(
				`GetRepoPath: not a git repository: "`+cfg.Storages[0].Path+`/bar"`,
				`mutator call: route repository mutator: get repository id: repository "default"/"bar" not found`,
			)),
		},
		{
			desc:        "no revision provided",
			repo:        repo,
			revision:    []byte(""),
			expectedErr: status.Error(codes.InvalidArgument, "revision: empty revision"),
		},
		{
			desc:        "unknown revision",
			repo:        repo,
			revision:    []byte("not-existing-ref"),
			expectedErr: status.Error(codes.InvalidArgument, "revision does not exist"),
		},
		{
			desc:        "invalid revision",
			repo:        repo,
			revision:    []byte("--output=/meow"),
			expectedErr: status.Error(codes.InvalidArgument, "revision: revision can't start with '-'"),
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			req := &gitalypb.ApplyGitattributesRequest{Repository: test.repo, Revision: test.revision}
			_, err := client.ApplyGitattributes(ctx, req)
			testhelper.RequireGrpcError(t, test.expectedErr, err)
		})
	}
}

func assertGitattributesApplied(t *testing.T, ctx context.Context, client gitalypb.RepositoryServiceClient, testRepo *gitalypb.Repository, attributesPath string, revision, expectedContents []byte) {
	t.Helper()

	req := &gitalypb.ApplyGitattributesRequest{Repository: testRepo, Revision: revision}
	c, err := client.ApplyGitattributes(ctx, req)

	assert.NoError(t, err)
	assert.NotNil(t, c)

	contents, err := os.ReadFile(attributesPath)
	if expectedContents == nil {
		if !os.IsNotExist(err) {
			t.Error(err)
		}
	} else {
		if err != nil {
			t.Error(err)
		}

		if info, err := os.Stat(attributesPath); err == nil {
			actualFileMode := info.Mode()
			assert.Equal(t, attributesFileMode, actualFileMode)
		}

		assert.Equal(t, expectedContents, contents)
	}
}
