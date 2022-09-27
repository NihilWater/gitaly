package ref

import (
	"context"
	"errors"
	"fmt"
	"strings"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v15/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func (s *server) DeleteRefs(ctx context.Context, in *gitalypb.DeleteRefsRequest) (*gitalypb.DeleteRefsResponse, error) {
	if err := validateDeleteRefRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	repo := s.localrepo(in.GetRepository())

	refnames, err := s.refsToRemove(ctx, repo, in)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	updater, err := updateref.New(ctx, repo)
	if err != nil {
		if errors.Is(err, git.ErrInvalidArg) {
			return nil, helper.ErrInvalidArgument(err)
		}
		return nil, helper.ErrInternal(err)
	}

	voteHash := voting.NewVoteHash()

	if featureflag.DeleteRefsStructuredErrors.IsEnabled(ctx) {
		var invalidRefnames [][]byte

		for _, ref := range refnames {
			if err := git.ValidateRevision([]byte(ref)); err != nil {
				invalidRefnames = append(invalidRefnames, []byte(ref))
			}
		}

		if len(invalidRefnames) > 0 {
			detailedErr, err := helper.ErrWithDetails(
				helper.ErrInvalidArgumentf("invalid references"),
				&gitalypb.DeleteRefsError{
					Error: &gitalypb.DeleteRefsError_InvalidFormat{
						InvalidFormat: &gitalypb.InvalidRefFormatError{
							Refs: invalidRefnames,
						},
					},
				},
			)
			if err != nil {
				return nil, helper.ErrInternalf("error details: %w", err)
			}

			return nil, detailedErr
		}
	}

	for _, ref := range refnames {
		if err := updater.Delete(ref); err != nil {
			if featureflag.DeleteRefsStructuredErrors.IsEnabled(ctx) {
				return nil, helper.ErrInternalf("unable to delete refs: %w", err)
			}

			return &gitalypb.DeleteRefsResponse{GitError: err.Error()}, nil
		}

		if _, err := voteHash.Write([]byte(ref.String() + "\n")); err != nil {
			return nil, helper.ErrInternalf("could not update vote hash: %w", err)
		}
	}

	if err := updater.Prepare(); err != nil {
		if featureflag.DeleteRefsStructuredErrors.IsEnabled(ctx) {
			var errAlreadyLocked *updateref.ErrAlreadyLocked
			if errors.As(err, &errAlreadyLocked) {
				detailedErr, err := helper.ErrWithDetails(
					helper.ErrFailedPreconditionf("cannot lock references"),
					&gitalypb.DeleteRefsError{
						Error: &gitalypb.DeleteRefsError_ReferencesLocked{
							ReferencesLocked: &gitalypb.ReferencesLockedError{
								Refs: [][]byte{[]byte(errAlreadyLocked.Ref)},
							},
						},
					},
				)
				if err != nil {
					return nil, helper.ErrInternalf("error details: %w", err)
				}

				return nil, detailedErr
			}

			return nil, helper.ErrInternalf("unable to prepare: %w", err)
		}

		return &gitalypb.DeleteRefsResponse{
			GitError: fmt.Sprintf("unable to delete refs: %s", err.Error()),
		}, nil
	}

	vote, err := voteHash.Vote()
	if err != nil {
		return nil, helper.ErrInternalf("could not compute vote: %w", err)
	}

	// All deletes we're doing in this RPC are force deletions. Because we're required to filter
	// out transactions which only consist of force deletions, we never do any voting via the
	// reference-transaction hook here. Instead, we need to resort to a manual vote which is
	// simply the concatenation of all reference we're about to delete.
	if err := transaction.VoteOnContext(ctx, s.txManager, vote, voting.Prepared); err != nil {
		return nil, helper.ErrInternalf("preparatory vote: %w", err)
	}

	if err := updater.Commit(); err != nil {
		if featureflag.DeleteRefsStructuredErrors.IsEnabled(ctx) {
			return nil, helper.ErrInternalf("unable to commit: %w", err)
		}

		return &gitalypb.DeleteRefsResponse{GitError: fmt.Sprintf("unable to delete refs: %s", err.Error())}, nil
	}

	if err := transaction.VoteOnContext(ctx, s.txManager, vote, voting.Committed); err != nil {
		return nil, helper.ErrInternalf("committing vote: %w", err)
	}

	return &gitalypb.DeleteRefsResponse{}, nil
}

func (s *server) refsToRemove(ctx context.Context, repo *localrepo.Repo, req *gitalypb.DeleteRefsRequest) ([]git.ReferenceName, error) {
	if len(req.Refs) > 0 {
		refs := make([]git.ReferenceName, len(req.Refs))
		for i, ref := range req.Refs {
			refs[i] = git.ReferenceName(ref)
		}
		return refs, nil
	}

	prefixes := make([]string, len(req.ExceptWithPrefix))
	for i, prefix := range req.ExceptWithPrefix {
		prefixes[i] = string(prefix)
	}

	existingRefs, err := repo.GetReferences(ctx)
	if err != nil {
		return nil, err
	}

	var refs []git.ReferenceName
	for _, existingRef := range existingRefs {
		if hasAnyPrefix(existingRef.Name.String(), prefixes) {
			continue
		}

		refs = append(refs, existingRef.Name)
	}

	return refs, nil
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}

	return false
}

func validateDeleteRefRequest(req *gitalypb.DeleteRefsRequest) error {
	if req.GetRepository() == nil {
		return gitalyerrors.ErrEmptyRepository
	}

	if len(req.ExceptWithPrefix) > 0 && len(req.Refs) > 0 {
		return errors.New("ExceptWithPrefix and Refs are mutually exclusive")
	}

	if len(req.ExceptWithPrefix) == 0 && len(req.Refs) == 0 { // You can't delete all refs
		return errors.New("empty ExceptWithPrefix and Refs")
	}

	for _, prefix := range req.ExceptWithPrefix {
		if len(prefix) == 0 {
			return errors.New("empty prefix for exclusion")
		}
	}

	for _, ref := range req.Refs {
		if len(ref) == 0 {
			return errors.New("empty ref")
		}
	}

	return nil
}
