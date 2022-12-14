package git2go

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/repository"
)

// RebaseCommand contains parameters to rebase a branch.
type RebaseCommand struct {
	// Repository is the path to execute rebase in.
	Repository string
	// Committer contains the the committer signature.
	Committer Signature
	// BranchName is the branch that is rebased. Deprecated, can be removed in the next release.
	BranchName string
	// UpstreamRevision is the revision where the branch is rebased onto. Deprecated, can be
	// removed in the next release.
	UpstreamRevision string
	// CommitID is the object ID of the commit that shall be rebased. Deprecates BranchName.
	CommitID git.ObjectID
	// UpstreamCommitID is the object ID of the commit which is considered to be the
	// upstream branch. This parameter determines both the commit onto which we're
	// about to rebase, which is the merge base of the upstream commit and rebased
	// commit, and which commits should be rebased, which is the commit range
	// upstream..commit. Deprecates the UpstreamRevision.
	UpstreamCommitID git.ObjectID
	// SkipEmptyCommits will cause commits which have already been applied on the target branch
	// and which are thus empty to be skipped. If unset, empty commits will cause the rebase to
	// fail.
	SkipEmptyCommits bool
	// SigningKey is a path to the key to sign commit using OpenPGP
	SigningKey string
}

// Rebase performs the rebase via gitaly-git2go
func (b *Executor) Rebase(ctx context.Context, repo repository.GitRepo, r RebaseCommand) (git.ObjectID, error) {
	r.SigningKey = b.signingKey

	return b.runWithGob(ctx, repo, "rebase", r)
}
