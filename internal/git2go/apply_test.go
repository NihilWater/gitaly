//go:build !gitaly_test_sha256

package git2go

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
)

func TestExecutor_Apply(t *testing.T) {
	ctx := testhelper.Context(t)
	cfg := testcfg.Build(t)
	testcfg.BuildGitalyGit2Go(t, cfg)

	repoProto, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
		SkipCreationViaService: true,
	})

	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	executor := NewExecutor(cfg, gittest.NewCommandFactory(t, cfg), config.NewLocator(cfg))

	oidBase, err := repo.WriteBlob(ctx, "file", strings.NewReader("base"))
	require.NoError(t, err)

	oidA, err := repo.WriteBlob(ctx, "file", strings.NewReader("a"))
	require.NoError(t, err)

	oidB, err := repo.WriteBlob(ctx, "file", strings.NewReader("b"))
	require.NoError(t, err)

	author := NewSignature("Test Author", "test.author@example.com", time.Now())
	committer := NewSignature("Test Committer", "test.committer@example.com", time.Now())

	parentCommitSHA, err := executor.Commit(ctx, repo, CommitCommand{
		Repository: repoPath,
		Author:     author,
		Committer:  committer,
		Message:    "base commit",
		Actions:    []Action{CreateFile{Path: "file", OID: oidBase.String()}},
	})
	require.NoError(t, err)

	noCommonAncestor, err := executor.Commit(ctx, repo, CommitCommand{
		Repository: repoPath,
		Author:     author,
		Committer:  committer,
		Message:    "commit with ab",
		Actions:    []Action{CreateFile{Path: "file", OID: oidA.String()}},
	})
	require.NoError(t, err)

	updateToA, err := executor.Commit(ctx, repo, CommitCommand{
		Repository: repoPath,
		Author:     author,
		Committer:  committer,
		Message:    "commit with a",
		Parent:     parentCommitSHA.String(),
		Actions:    []Action{UpdateFile{Path: "file", OID: oidA.String()}},
	})
	require.NoError(t, err)

	updateToB, err := executor.Commit(ctx, repo, CommitCommand{
		Repository: repoPath,
		Author:     author,
		Committer:  committer,
		Message:    "commit to b",
		Parent:     parentCommitSHA.String(),
		Actions:    []Action{UpdateFile{Path: "file", OID: oidB.String()}},
	})
	require.NoError(t, err)

	updateFromAToB, err := executor.Commit(ctx, repo, CommitCommand{
		Repository: repoPath,
		Author:     author,
		Committer:  committer,
		Message:    "commit a -> b",
		Parent:     updateToA.String(),
		Actions:    []Action{UpdateFile{Path: "file", OID: oidB.String()}},
	})
	require.NoError(t, err)

	otherFile, err := executor.Commit(ctx, repo, CommitCommand{
		Repository: repoPath,
		Author:     author,
		Committer:  committer,
		Message:    "commit with other-file",
		Actions:    []Action{CreateFile{Path: "other-file", OID: oidA.String()}},
	})
	require.NoError(t, err)

	diffBetween := func(tb testing.TB, fromCommit, toCommit git.ObjectID) []byte {
		tb.Helper()
		return gittest.Exec(tb, cfg, "-C", repoPath, "format-patch", "--stdout", fromCommit.String()+".."+toCommit.String())
	}

	for _, tc := range []struct {
		desc         string
		patches      []Patch
		parentCommit git.ObjectID
		tree         []gittest.TreeEntry
		error        error
	}{
		{
			desc: "patch applies cleanly",
			patches: []Patch{
				{
					Author:  author,
					Message: "test commit message",
					Diff:    diffBetween(t, parentCommitSHA, updateToA),
				},
			},
			parentCommit: parentCommitSHA,
			tree: []gittest.TreeEntry{
				{Path: "file", Mode: "100644", Content: "a"},
			},
		},
		{
			desc: "multiple patches apply cleanly",
			patches: []Patch{
				{
					Author:  author,
					Message: "commit with a",
					Diff:    diffBetween(t, parentCommitSHA, updateToA),
				},
				{
					Author:  author,
					Message: "commit with a -> b",
					Diff:    diffBetween(t, updateToA, updateFromAToB),
				},
			},
			parentCommit: updateToA,
			tree: []gittest.TreeEntry{
				{Path: "file", Mode: "100644", Content: "b"},
			},
		},
		{
			desc: "three way merged",
			patches: []Patch{
				{
					Author:  author,
					Message: "three-way merged files",
					Diff:    diffBetween(t, parentCommitSHA, otherFile),
				},
			},
			parentCommit: parentCommitSHA,
			tree: []gittest.TreeEntry{
				{Path: "file", Mode: "100644", Content: "base"},
				{Path: "other-file", Mode: "100644", Content: "a"},
			},
		},
		{
			// This test asserts incorrect behavior due to a bug in libgit2's
			// git_apply_to_tree function. The correct behavior would be to
			// return an error about merge conflict but we currently concatenate
			// the blobs of the two trees together. This test will fail once the
			// issue is fixed.
			//
			// See: https://gitlab.com/gitlab-org/gitaly/-/issues/3325
			desc: "no common ancestor",
			patches: []Patch{
				{
					Author:  author,
					Message: "three-way merged file",
					Diff:    diffBetween(t, parentCommitSHA, noCommonAncestor),
				},
			},
			parentCommit: parentCommitSHA,
			// error: ErrMergeConflict, <- correct output
			tree: []gittest.TreeEntry{
				{Path: "file", Mode: "100644", Content: "abase"},
			},
		},
		{
			desc: "merge conflict",
			patches: []Patch{
				{
					Author:  author,
					Message: "applies cleanly",
					Diff:    diffBetween(t, parentCommitSHA, updateToA),
				},
				{
					Author:  author,
					Message: "conflicts",
					Diff:    diffBetween(t, parentCommitSHA, updateToB),
				},
			},
			error: ErrMergeConflict,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			commitID, err := executor.Apply(ctx, repo, ApplyParams{
				Repository:   repoPath,
				Committer:    committer,
				ParentCommit: parentCommitSHA.String(),
				Patches:      NewSlicePatchIterator(tc.patches),
			})
			if tc.error != nil {
				require.True(t, errors.Is(err, tc.error), err)
				return
			}

			require.Equal(t, commit{
				Parent:    tc.parentCommit,
				Author:    author,
				Committer: committer,
				Message:   tc.patches[len(tc.patches)-1].Message,
			}, getCommit(t, ctx, repo, commitID, false))
			gittest.RequireTree(t, cfg, repoPath, commitID.String(), tc.tree)
		})
	}
}
