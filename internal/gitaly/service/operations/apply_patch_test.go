//go:build !gitaly_test_sha256

package operations

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestUserApplyPatch(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	ctx, cfg, _, _, client := setupOperationsService(t, ctx)

	type actionFunc func(testing.TB, *localrepo.Repo) git2go.Action

	createFile := func(filepath string, content string) actionFunc {
		return func(tb testing.TB, repo *localrepo.Repo) git2go.Action {
			fileOID, err := repo.WriteBlob(ctx, filepath, strings.NewReader(content))
			require.NoError(tb, err)

			return git2go.CreateFile{Path: filepath, OID: fileOID.String()}
		}
	}

	updateFile := func(filepath string, content string) actionFunc {
		return func(tb testing.TB, repo *localrepo.Repo) git2go.Action {
			fileOID, err := repo.WriteBlob(ctx, filepath, strings.NewReader(content))
			require.NoError(tb, err)

			return git2go.UpdateFile{Path: filepath, OID: fileOID.String()}
		}
	}

	moveFile := func(oldPath, newPath string) actionFunc {
		return func(testing.TB, *localrepo.Repo) git2go.Action {
			return git2go.MoveFile{Path: oldPath, NewPath: newPath}
		}
	}

	deleteFile := func(filepath string) actionFunc {
		return func(testing.TB, *localrepo.Repo) git2go.Action {
			return git2go.DeleteFile{Path: filepath}
		}
	}

	// commitActions represents actions taken to build a commit.
	type commitActions []actionFunc

	errPatchingFailed := status.Error(
		codes.FailedPrecondition,
		`Patch failed at 0002 commit subject
When you have resolved this problem, run "git am --continue".
If you prefer to skip this patch, run "git am --skip" instead.
To restore the original branch and stop patching, run "git am --abort".
`)

	for _, tc := range []struct {
		desc string
		// sends a request to a non-existent repository
		nonExistentRepository bool
		// baseCommit is the commit which the patch commitActions are applied against.
		baseCommit commitActions
		// baseReference is the branch where baseCommit is, by default "master"
		baseReference git.ReferenceName
		// notSentByAuthor marks the patch as being sent by someone else than the author.
		notSentByAuthor bool
		// targetBranch is the branch where the patched commit goes
		targetBranch string
		// extraBranches are created with empty commits for verifying the correct base branch
		// gets selected.
		extraBranches []string
		// patches describe how to build each commit that gets applied as a patch.
		// Each patch is series of actions that are applied on top of the baseCommit.
		// Each action produces one commit. The patch is then generated from the last commit
		// in the series to its parent.
		//
		// After the patches are generated, they are applied sequentially on the base commit.
		patches       []commitActions
		error         error
		branchCreated bool
		tree          []gittest.TreeEntry
	}{
		{
			desc:                  "non-existent repository",
			targetBranch:          "master",
			nonExistentRepository: true,
			error: func() error {
				if testhelper.IsPraefectEnabled() {
					return status.Errorf(codes.NotFound, "mutator call: route repository mutator: get repository id: repository %q/%q not found", cfg.Storages[0].Name, "doesnt-exist")
				}

				return status.Errorf(codes.NotFound, "GetRepoPath: not a git repository: \"%s/%s\"", cfg.Storages[0].Path, "doesnt-exist")
			}(),
		},
		{
			desc:         "creating the first branch does not work",
			targetBranch: "master",
			patches: []commitActions{
				{
					createFile("file", "base-content"),
				},
			},
			error: status.Error(codes.Internal, "no default branch"),
		},
		{
			desc:          "creating a new branch from HEAD works",
			baseCommit:    commitActions{createFile("file", "base-content")},
			baseReference: "HEAD",
			extraBranches: []string{"refs/heads/master", "refs/heads/some-extra-branch"},
			targetBranch:  "new-branch",
			patches: []commitActions{
				{
					updateFile("file", "patch 1"),
				},
			},
			branchCreated: true,
			tree: []gittest.TreeEntry{
				{Mode: "100644", Path: "file", Content: "patch 1"},
			},
		},
		{
			desc:          "creating a new branch from the first listed branch works",
			baseCommit:    commitActions{createFile("file", "base-content")},
			baseReference: "refs/heads/a",
			extraBranches: []string{"refs/heads/b"},
			targetBranch:  "new-branch",
			patches: []commitActions{
				{
					updateFile("file", "patch 1"),
				},
			},
			branchCreated: true,
			tree: []gittest.TreeEntry{
				{Mode: "100644", Path: "file", Content: "patch 1"},
			},
		},
		{
			desc:          "multiple patches apply cleanly",
			baseCommit:    commitActions{createFile("file", "base-content")},
			baseReference: "refs/heads/master",
			targetBranch:  "master",
			patches: []commitActions{
				{
					updateFile("file", "patch 1"),
				},
				{
					updateFile("file", "patch 1"),
					updateFile("file", "patch 2"),
				},
			},
			tree: []gittest.TreeEntry{
				{Mode: "100644", Path: "file", Content: "patch 2"},
			},
		},
		{
			desc:            "author in from field in body set correctly",
			baseCommit:      commitActions{createFile("file", "base-content")},
			baseReference:   "refs/heads/master",
			notSentByAuthor: true,
			targetBranch:    "master",
			patches: []commitActions{
				{
					updateFile("file", "patch 1"),
				},
			},
			tree: []gittest.TreeEntry{
				{Mode: "100644", Path: "file", Content: "patch 1"},
			},
		},
		{
			desc:          "multiple patches apply via fallback three-way merge",
			baseCommit:    commitActions{createFile("file", "base-content")},
			baseReference: "refs/heads/master",
			targetBranch:  "master",
			patches: []commitActions{
				{
					updateFile("file", "patch 1"),
				},
				{
					updateFile("file", "patch 2"),
					updateFile("file", "patch 1"),
				},
			},
			tree: []gittest.TreeEntry{
				{Mode: "100644", Path: "file", Content: "patch 1"},
			},
		},
		{
			desc:          "patching fails due to modify-modify conflict",
			baseCommit:    commitActions{createFile("file", "base-content")},
			baseReference: "refs/heads/master",
			targetBranch:  "master",
			patches: []commitActions{
				{
					updateFile("file", "patch 1"),
				},
				{
					updateFile("file", "patch 2"),
				},
			},
			error: errPatchingFailed,
		},
		{
			desc:          "patching fails due to add-add conflict",
			baseCommit:    commitActions{createFile("file", "base-content")},
			baseReference: "refs/heads/master",
			targetBranch:  "master",
			patches: []commitActions{
				{
					createFile("added-file", "content-1"),
				},
				{
					createFile("added-file", "content-2"),
				},
			},
			error: errPatchingFailed,
		},
		{
			desc:          "patch applies using rename detection",
			baseCommit:    commitActions{createFile("file", "line 1\nline 2\nline 3\nline 4\n")},
			baseReference: "refs/heads/master",
			targetBranch:  "master",
			patches: []commitActions{
				{
					moveFile("file", "moved-file"),
				},
				{
					updateFile("file", "line 1\nline 2\nline 3\nline 4\nadded\n"),
				},
			},
			tree: []gittest.TreeEntry{
				{Mode: "100644", Path: "moved-file", Content: "line 1\nline 2\nline 3\nline 4\nadded\n"},
			},
		},
		{
			desc:          "patching fails due to delete-modify conflict",
			baseCommit:    commitActions{createFile("file", "base-content")},
			baseReference: "refs/heads/master",
			targetBranch:  "master",
			patches: []commitActions{
				{
					deleteFile("file"),
				},
				{
					updateFile("file", "updated content"),
				},
			},
			error: errPatchingFailed,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoPb, repoPath := gittest.CreateRepository(t, ctx, cfg)

			repo := localrepo.NewTestRepo(t, cfg, repoPb)
			rewrittenRepo := gittest.RewrittenRepository(t, ctx, cfg, repoPb)

			executor := git2go.NewExecutor(cfg, gittest.NewCommandFactory(t, cfg), config.NewLocator(cfg))

			authorTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			committerTime := authorTime.Add(time.Hour)
			author := git2go.NewSignature("Test Author", "author@example.com", authorTime)
			committer := git2go.NewSignature("Overridden By Request User", "overridden@example.com", committerTime)
			commitMessage := "commit subject\n\n\ncommit message body\n\n\n"

			var baseCommit git.ObjectID
			for _, action := range tc.baseCommit {
				var err error
				baseCommit, err = executor.Commit(ctx, rewrittenRepo, git2go.CommitCommand{
					Repository: repoPath,
					Author:     author,
					Committer:  committer,
					Message:    commitMessage,
					Parent:     baseCommit.String(),
					Actions:    []git2go.Action{action(t, repo)},
				})
				require.NoError(t, err)
			}

			if baseCommit != "" {
				require.NoError(t, repo.UpdateRef(ctx, tc.baseReference, baseCommit, git.ObjectHashSHA1.ZeroOID))
			}

			if tc.extraBranches != nil {
				emptyCommit, err := executor.Commit(ctx, rewrittenRepo, git2go.CommitCommand{
					Repository: repoPath,
					Author:     author,
					Committer:  committer,
					Message:    "empty commit",
				})
				require.NoError(t, err)

				for _, extraBranch := range tc.extraBranches {
					require.NoError(t, repo.UpdateRef(ctx,
						git.NewReferenceNameFromBranchName(extraBranch), emptyCommit, git.ObjectHashSHA1.ZeroOID),
					)
				}
			}

			var patches [][]byte
			for _, commitActions := range tc.patches {
				commit := baseCommit
				for _, action := range commitActions {
					var err error
					commit, err = executor.Commit(ctx, rewrittenRepo, git2go.CommitCommand{
						Repository: repoPath,
						Author:     author,
						Committer:  committer,
						Message:    commitMessage,
						Parent:     commit.String(),
						Actions:    []git2go.Action{action(t, repo)},
					})
					require.NoError(t, err)
				}

				formatPatchArgs := []string{"-C", repoPath, "format-patch", "--stdout"}
				if tc.notSentByAuthor {
					formatPatchArgs = append(formatPatchArgs, "--from=Test Sender <sender@example.com>")
				}

				if baseCommit == "" {
					formatPatchArgs = append(formatPatchArgs, "--root", commit.String())
				} else {
					formatPatchArgs = append(formatPatchArgs, commit.String()+"~1.."+commit.String())
				}

				patches = append(patches, gittest.Exec(t, cfg, formatPatchArgs...))
			}

			stream, err := client.UserApplyPatch(ctx)
			require.NoError(t, err)

			requestTime := committerTime.Add(time.Hour)
			requestTimestamp := timestamppb.New(requestTime)

			if tc.nonExistentRepository {
				repoPb.RelativePath = "doesnt-exist"
			}

			require.NoError(t, stream.Send(&gitalypb.UserApplyPatchRequest{
				UserApplyPatchRequestPayload: &gitalypb.UserApplyPatchRequest_Header_{
					Header: &gitalypb.UserApplyPatchRequest_Header{
						Repository:   repoPb,
						User:         gittest.TestUser,
						TargetBranch: []byte(tc.targetBranch),
						Timestamp:    requestTimestamp,
					},
				},
			}))

		outerLoop:
			for _, patch := range patches {
				// we stream the patches one rune at a time to exercise the streaming code
				for _, r := range patch {
					err := stream.Send(&gitalypb.UserApplyPatchRequest{
						UserApplyPatchRequestPayload: &gitalypb.UserApplyPatchRequest_Patches{
							Patches: []byte{r},
						},
					})

					// In case the request we're sending to the server results
					// in an error it can happen that the server already noticed
					// the request and thus returned an error while we're still
					// streaming the actual patch data. If so, the server closes
					// the stream and we are left unable to continue sending,
					// which means we get an EOF here.
					//
					// Abort the loop here so that we can observe the actual
					// error in `CloseAndRecv()`.
					if err == io.EOF {
						break outerLoop
					}

					require.NoError(t, err)
				}
			}

			actualResponse, err := stream.CloseAndRecv()
			if tc.error != nil {
				testhelper.RequireGrpcError(t, tc.error, err)
				return
			}

			require.NoError(t, err)

			commitID := actualResponse.GetBranchUpdate().GetCommitId()
			actualResponse.GetBranchUpdate().CommitId = ""
			testhelper.ProtoEqual(t, &gitalypb.UserApplyPatchResponse{
				BranchUpdate: &gitalypb.OperationBranchUpdate{
					RepoCreated:   false,
					BranchCreated: tc.branchCreated,
				},
			}, actualResponse)

			targetBranchCommit, err := repo.ResolveRevision(ctx,
				git.NewReferenceNameFromBranchName(tc.targetBranch).Revision()+"^{commit}")
			require.NoError(t, err)
			require.Equal(t, targetBranchCommit.String(), commitID)

			actualCommit, err := repo.ReadCommit(ctx, git.Revision(commitID))
			require.NoError(t, err)
			require.NotEmpty(t, actualCommit.ParentIds)
			actualCommit.ParentIds = nil // the parent changes with the patches, we just check it is set
			actualCommit.TreeId = ""     // treeID is asserted via its contents below

			expectedBody := []byte("commit subject\n\ncommit message body\n")
			testhelper.ProtoEqual(t,
				&gitalypb.GitCommit{
					Id:      commitID,
					Subject: []byte("commit subject"),
					Body:    expectedBody,
					Author: &gitalypb.CommitAuthor{
						Name:     []byte("Test Author"),
						Email:    []byte("author@example.com"),
						Date:     timestamppb.New(authorTime),
						Timezone: []byte("+0000"),
					},
					Committer: &gitalypb.CommitAuthor{
						Name:     gittest.TestUser.Name,
						Email:    gittest.TestUser.Email,
						Date:     requestTimestamp,
						Timezone: []byte("+0800"),
					},
					BodySize: int64(len(expectedBody)),
				},
				actualCommit,
			)

			gittest.RequireTree(t, cfg, repoPath, commitID, tc.tree)
		})
	}
}

func TestUserApplyPatch_successful(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, repoPath, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	testPatchReadme := "testdata/0001-A-commit-from-a-patch.patch"
	testPatchFeature := "testdata/0001-This-does-not-apply-to-the-feature-branch.patch"

	testCases := []struct {
		desc           string
		branchName     string
		branchCreated  bool
		patches        []string
		commitMessages []string
	}{
		{
			desc:           "a new branch",
			branchName:     "patched-branch",
			branchCreated:  true,
			patches:        []string{testPatchReadme},
			commitMessages: []string{"A commit from a patch"},
		},
		{
			desc:           "an existing branch",
			branchName:     "feature",
			branchCreated:  false,
			patches:        []string{testPatchReadme},
			commitMessages: []string{"A commit from a patch"},
		},
		{
			desc:           "multiple patches",
			branchName:     "branch-with-multiple-patches",
			branchCreated:  true,
			patches:        []string{testPatchReadme, testPatchFeature},
			commitMessages: []string{"A commit from a patch", "This does not apply to the `feature` branch"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			stream, err := client.UserApplyPatch(ctx)
			require.NoError(t, err)

			headerRequest := applyPatchHeaderRequest(repoProto, gittest.TestUser, testCase.branchName)
			require.NoError(t, stream.Send(headerRequest))

			writer := streamio.NewWriter(func(p []byte) error {
				patchRequest := applyPatchPatchesRequest(p)

				return stream.Send(patchRequest)
			})

			for _, patchFileName := range testCase.patches {
				func() {
					file, err := os.Open(patchFileName)
					require.NoError(t, err)
					defer file.Close()

					byteReader := iotest.OneByteReader(file)
					_, err = io.Copy(writer, byteReader)
					require.NoError(t, err)
				}()
			}

			response, err := stream.CloseAndRecv()
			require.NoError(t, err)

			response.GetBranchUpdate()
			require.Equal(t, testCase.branchCreated, response.GetBranchUpdate().GetBranchCreated())

			branches := gittest.Exec(t, cfg, "-C", repoPath, "branch")
			require.Contains(t, string(branches), testCase.branchName)

			maxCount := fmt.Sprintf("--max-count=%d", len(testCase.commitMessages))

			gitArgs := []string{
				"-C",
				repoPath,
				"log",
				testCase.branchName,
				"--format=%H",
				maxCount,
				"--reverse",
			}

			output := gittest.Exec(t, cfg, gitArgs...)
			shas := strings.Split(string(output), "\n")
			// Throw away the last element, as that's going to be
			// an empty string.
			if len(shas) > 0 {
				shas = shas[:len(shas)-1]
			}

			for index, sha := range shas {
				commit, err := repo.ReadCommit(ctx, git.Revision(sha))
				require.NoError(t, err)

				require.NotNil(t, commit)
				require.Equal(t, string(commit.Subject), testCase.commitMessages[index])
				require.Equal(t, string(commit.Author.Email), "patchuser@gitlab.org")
				require.Equal(t, string(commit.Committer.Email), string(gittest.TestUser.Email))
			}
		})
	}
}

func TestUserApplyPatch_stableID(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	ctx, cfg, repoProto, _, client := setupOperationsService(t, ctx)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	stream, err := client.UserApplyPatch(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.UserApplyPatchRequest{
		UserApplyPatchRequestPayload: &gitalypb.UserApplyPatchRequest_Header_{
			Header: &gitalypb.UserApplyPatchRequest_Header{
				Repository:   repoProto,
				User:         gittest.TestUser,
				TargetBranch: []byte("branch"),
				Timestamp:    &timestamppb.Timestamp{Seconds: 1234512345},
			},
		},
	}))

	patch := testhelper.MustReadFile(t, "testdata/0001-A-commit-from-a-patch.patch")
	require.NoError(t, stream.Send(&gitalypb.UserApplyPatchRequest{
		UserApplyPatchRequestPayload: &gitalypb.UserApplyPatchRequest_Patches{
			Patches: patch,
		},
	}))

	response, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.True(t, response.BranchUpdate.BranchCreated)

	patchedCommit, err := repo.ReadCommit(ctx, git.Revision("branch"))
	require.NoError(t, err)
	require.Equal(t, &gitalypb.GitCommit{
		Id:     "93285a1e2319749f6d2bb7c394451a70ca7dcd07",
		TreeId: "98091f327a9fb132fcb4b490a420c276c653c4c6",
		ParentIds: []string{
			"1e292f8fedd741b75372e19097c76d327140c312",
		},
		Subject:  []byte("A commit from a patch"),
		Body:     []byte("A commit from a patch\n"),
		BodySize: 22,
		Author: &gitalypb.CommitAuthor{
			Name:     []byte("Patch User"),
			Email:    []byte("patchuser@gitlab.org"),
			Date:     &timestamppb.Timestamp{Seconds: 1539862835},
			Timezone: []byte("+0200"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     gittest.TestUser.Name,
			Email:    gittest.TestUser.Email,
			Date:     &timestamppb.Timestamp{Seconds: 1234512345},
			Timezone: []byte("+0800"),
		},
	}, patchedCommit)
}

func TestUserApplyPatch_transactional(t *testing.T) {
	t.Parallel()

	txManager := transaction.NewTrackingManager()

	ctx := testhelper.Context(t)

	ctx, _, repoProto, _, client := setupOperationsService(t, ctx, testserver.WithTransactionManager(txManager))

	// Reset the transaction manager as the setup call above creates a repository which
	// ends up creating some votes with Praefect enabled.
	txManager.Reset()

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = peer.NewContext(ctx, &peer.Peer{
		AuthInfo: backchannel.WithID(nil, 1234),
	})
	ctx = metadata.IncomingToOutgoing(ctx)

	patch := testhelper.MustReadFile(t, "testdata/0001-A-commit-from-a-patch.patch")

	stream, err := client.UserApplyPatch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.UserApplyPatchRequest{
		UserApplyPatchRequestPayload: &gitalypb.UserApplyPatchRequest_Header_{
			Header: &gitalypb.UserApplyPatchRequest_Header{
				Repository:   repoProto,
				User:         gittest.TestUser,
				TargetBranch: []byte("branch"),
				Timestamp:    &timestamppb.Timestamp{Seconds: 1234512345},
			},
		},
	}))
	require.NoError(t, stream.Send(&gitalypb.UserApplyPatchRequest{
		UserApplyPatchRequestPayload: &gitalypb.UserApplyPatchRequest_Patches{
			Patches: patch,
		},
	}))
	response, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.True(t, response.BranchUpdate.BranchCreated)

	require.Equal(t, 12, len(txManager.Votes()))
}

func TestFailedPatchApplyPatch(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	ctx, _, repo, _, client := setupOperationsService(t, ctx)

	testPatch := testhelper.MustReadFile(t, "testdata/0001-This-does-not-apply-to-the-feature-branch.patch")

	stream, err := client.UserApplyPatch(ctx)
	require.NoError(t, err)

	headerRequest := applyPatchHeaderRequest(repo, gittest.TestUser, "feature")
	require.NoError(t, stream.Send(headerRequest))

	patchRequest := applyPatchPatchesRequest(testPatch)
	require.NoError(t, stream.Send(patchRequest))

	_, err = stream.CloseAndRecv()
	testhelper.RequireGrpcCode(t, err, codes.FailedPrecondition)
}

func TestFailedValidationUserApplyPatch(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)
	ctx, _, repo, _, client := setupOperationsService(t, ctx)

	testCases := []struct {
		desc        string
		repo        *gitalypb.Repository
		user        *gitalypb.User
		branchName  string
		expectedErr error
	}{
		{
			desc:       "missing Repository",
			branchName: "new-branch",
			user:       gittest.TestUser,
			expectedErr: status.Error(codes.InvalidArgument, testhelper.GitalyOrPraefectMessage(
				"empty Repository",
				"repo scoped: empty Repository",
			)),
		},
		{
			desc:        "missing Branch",
			repo:        repo,
			user:        gittest.TestUser,
			expectedErr: status.Error(codes.InvalidArgument, "missing Branch"),
		},
		{
			desc:        "empty BranchName",
			repo:        repo,
			user:        gittest.TestUser,
			branchName:  "",
			expectedErr: status.Error(codes.InvalidArgument, "missing Branch"),
		},
		{
			desc:        "missing User",
			branchName:  "new-branch",
			repo:        repo,
			expectedErr: status.Error(codes.InvalidArgument, "missing User"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			stream, err := client.UserApplyPatch(ctx)
			require.NoError(t, err)
			err = stream.Send(&gitalypb.UserApplyPatchRequest{
				UserApplyPatchRequestPayload: &gitalypb.UserApplyPatchRequest_Header_{
					Header: &gitalypb.UserApplyPatchRequest_Header{
						Repository:   testCase.repo,
						User:         testCase.user,
						TargetBranch: []byte(testCase.branchName),
					},
				},
			})
			require.NoError(t, err)
			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, testCase.expectedErr, err)
		})
	}
}

func applyPatchHeaderRequest(repo *gitalypb.Repository, user *gitalypb.User, branch string) *gitalypb.UserApplyPatchRequest {
	header := &gitalypb.UserApplyPatchRequest_Header_{
		Header: &gitalypb.UserApplyPatchRequest_Header{
			Repository:   repo,
			User:         user,
			TargetBranch: []byte(branch),
		},
	}
	return &gitalypb.UserApplyPatchRequest{
		UserApplyPatchRequestPayload: header,
	}
}

func applyPatchPatchesRequest(patches []byte) *gitalypb.UserApplyPatchRequest {
	requestPatches := &gitalypb.UserApplyPatchRequest_Patches{
		Patches: patches,
	}

	return &gitalypb.UserApplyPatchRequest{
		UserApplyPatchRequestPayload: requestPatches,
	}
}
