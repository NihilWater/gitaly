package operations

import (
	"context"
	"errors"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func validateMergeBranchRequest(request *gitalypb.UserMergeBranchRequest) error {
	if err := service.ValidateRepository(request.GetRepository()); err != nil {
		return err
	}

	if request.User == nil {
		return errors.New("empty user")
	}

	if len(request.User.Email) == 0 {
		return errors.New("empty user email")
	}

	if len(request.User.Name) == 0 {
		return errors.New("empty user name")
	}

	if len(request.Branch) == 0 {
		return errors.New("empty branch name")
	}

	if request.CommitId == "" {
		return errors.New("empty commit ID")
	}

	if len(request.Message) == 0 {
		return errors.New("empty message")
	}

	return nil
}

//nolint:revive // This is unintentionally missing documentation.
func (s *Server) UserMergeBranch(stream gitalypb.OperationService_UserMergeBranchServer) error {
	ctx := stream.Context()

	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	if err := validateMergeBranchRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, firstRequest.GetRepository())
	if err != nil {
		return err
	}

	repoPath, err := quarantineRepo.Path()
	if err != nil {
		return err
	}

	referenceName := git.NewReferenceNameFromBranchName(string(firstRequest.Branch))

	revision, err := quarantineRepo.ResolveRevision(ctx, referenceName.Revision())
	if err != nil {
		if errors.Is(err, git.ErrReferenceNotFound) {
			return helper.ErrNotFound(err)
		}
		return helper.ErrInternal(err)
	}

	authorDate, err := dateFromProto(firstRequest)
	if err != nil {
		return helper.ErrInvalidArgument(err)
	}

	merge, err := s.git2goExecutor.Merge(ctx, quarantineRepo, git2go.MergeCommand{
		Repository: repoPath,
		AuthorName: string(firstRequest.User.Name),
		AuthorMail: string(firstRequest.User.Email),
		AuthorDate: authorDate,
		Message:    string(firstRequest.Message),
		Ours:       revision.String(),
		Theirs:     firstRequest.CommitId,
	})
	if err != nil {
		if errors.Is(err, git2go.ErrInvalidArgument) {
			return helper.ErrInvalidArgument(err)
		}

		var conflictErr git2go.ConflictingFilesError
		if errors.As(err, &conflictErr) {
			conflictingFiles := make([][]byte, 0, len(conflictErr.ConflictingFiles))
			for _, conflictingFile := range conflictErr.ConflictingFiles {
				conflictingFiles = append(conflictingFiles, []byte(conflictingFile))
			}

			detailedErr, err := helper.ErrWithDetails(
				helper.ErrFailedPreconditionf("merging commits: %w", err),
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_MergeConflict{
						MergeConflict: &gitalypb.MergeConflictError{
							ConflictingFiles: conflictingFiles,
							ConflictingCommitIds: []string{
								revision.String(),
								firstRequest.CommitId,
							},
						},
					},
				},
			)
			if err != nil {
				return helper.ErrInternalf("error details: %w", err)
			}

			return detailedErr
		}

		return helper.ErrInternal(err)
	}

	mergeOID, err := git.ObjectHashSHA1.FromHex(merge.CommitID)
	if err != nil {
		return helper.ErrInternalf("could not parse merge ID: %w", err)
	}

	if err := stream.Send(&gitalypb.UserMergeBranchResponse{
		CommitId: merge.CommitID,
	}); err != nil {
		return err
	}

	secondRequest, err := stream.Recv()
	if err != nil {
		return err
	}
	if !secondRequest.Apply {
		return helper.ErrFailedPreconditionf("merge aborted by client")
	}

	if err := s.updateReferenceWithHooks(ctx, firstRequest.GetRepository(), firstRequest.User, quarantineDir, referenceName, mergeOID, revision); err != nil {
		var notAllowedError hook.NotAllowedError
		var customHookErr updateref.CustomHookError
		var updateRefError updateref.Error

		if errors.As(err, &notAllowedError) {
			detailedErr, err := helper.ErrWithDetails(
				helper.ErrPermissionDenied(notAllowedError),
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_AccessCheck{
						AccessCheck: &gitalypb.AccessCheckError{
							ErrorMessage: notAllowedError.Message,
							UserId:       notAllowedError.UserID,
							Protocol:     notAllowedError.Protocol,
							Changes:      notAllowedError.Changes,
						},
					},
				},
			)
			if err != nil {
				return helper.ErrInternalf("error details: %w", err)
			}

			return detailedErr
		} else if errors.As(err, &customHookErr) {
			// When an error happens updating the reference, e.g. because of a
			// race with another update, then we should tell the user that a
			// precondition failed. A retry may fix this.
			detailedErr, err := helper.ErrWithDetails(
				helper.ErrPermissionDenied(customHookErr),
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_CustomHook{
						CustomHook: customHookErr.Proto(),
					},
				},
			)
			if err != nil {
				return helper.ErrInternalf("error details: %w", err)
			}

			return detailedErr
		} else if errors.As(err, &updateRefError) {
			// When an error happens updating the reference, e.g. because of a
			// race with another update, then we should tell the user that a
			// precondition failed. A retry may fix this.
			detailedErr, err := helper.ErrWithDetails(
				helper.ErrFailedPrecondition(updateRefError),
				&gitalypb.UserMergeBranchError{
					Error: &gitalypb.UserMergeBranchError_ReferenceUpdate{
						ReferenceUpdate: &gitalypb.ReferenceUpdateError{
							ReferenceName: []byte(updateRefError.Reference.String()),
							OldOid:        updateRefError.OldOID.String(),
							NewOid:        updateRefError.NewOID.String(),
						},
					},
				},
			)
			if err != nil {
				return helper.ErrInternalf("error details: %w", err)
			}

			return detailedErr
		}

		return helper.ErrInternal(err)
	}

	if err := stream.Send(&gitalypb.UserMergeBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      merge.CommitID,
			RepoCreated:   false,
			BranchCreated: false,
		},
	}); err != nil {
		return err
	}

	return nil
}

func validateFFRequest(in *gitalypb.UserFFBranchRequest) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}

	if len(in.Branch) == 0 {
		return errors.New("empty branch name")
	}

	if in.User == nil {
		return errors.New("empty user")
	}

	if in.CommitId == "" {
		return errors.New("empty commit id")
	}

	return nil
}

//nolint:revive // This is unintentionally missing documentation.
func (s *Server) UserFFBranch(ctx context.Context, in *gitalypb.UserFFBranchRequest) (*gitalypb.UserFFBranchResponse, error) {
	if err := validateFFRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	referenceName := git.NewReferenceNameFromBranchName(string(in.Branch))

	// While we're creating a quarantine directory, we know that it won't ever have any new
	// objects given that we're doing a fast-forward merge. We still want to create one such
	// that Rails can efficiently compute new objects.
	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, in.GetRepository())
	if err != nil {
		return nil, err
	}

	revision, err := quarantineRepo.ResolveRevision(ctx, referenceName.Revision())
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	commitID, err := git.ObjectHashSHA1.FromHex(in.CommitId)
	if err != nil {
		return nil, helper.ErrInvalidArgumentf("cannot parse commit ID: %w", err)
	}

	ancestor, err := quarantineRepo.IsAncestor(ctx, revision.Revision(), commitID.Revision())
	if err != nil {
		return nil, helper.ErrInternalf("checking for ancestry: %w", err)
	}
	if !ancestor {
		return nil, helper.ErrFailedPreconditionf("not fast forward")
	}

	if err := s.updateReferenceWithHooks(ctx, in.GetRepository(), in.User, quarantineDir, referenceName, commitID, revision); err != nil {
		var customHookErr updateref.CustomHookError
		if errors.As(err, &customHookErr) {
			return &gitalypb.UserFFBranchResponse{
				PreReceiveError: customHookErr.Error(),
			}, nil
		}

		var updateRefError updateref.Error
		if errors.As(err, &updateRefError) {
			// When an error happens updating the reference, e.g. because of a race
			// with another update, then Ruby code didn't send an error but just an
			// empty response.
			return &gitalypb.UserFFBranchResponse{}, nil
		}

		return nil, helper.ErrInternalf("updating ref with hooks: %w", err)
	}

	return &gitalypb.UserFFBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId: in.CommitId,
		},
	}, nil
}

func validateUserMergeToRefRequest(in *gitalypb.UserMergeToRefRequest) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}

	if len(in.FirstParentRef) == 0 && len(in.Branch) == 0 {
		return errors.New("empty first parent ref and branch name")
	}

	if in.User == nil {
		return errors.New("empty user")
	}

	if in.SourceSha == "" {
		return errors.New("empty source SHA")
	}

	if len(in.TargetRef) == 0 {
		return errors.New("empty target ref")
	}

	if !strings.HasPrefix(string(in.TargetRef), "refs/merge-requests") {
		return errors.New("invalid target ref")
	}

	return nil
}

// UserMergeToRef overwrites the given TargetRef to point to either Branch or
// FirstParentRef. Afterwards, it performs a merge of SourceSHA with either
// Branch or FirstParentRef and updates TargetRef to the merge commit.
func (s *Server) UserMergeToRef(ctx context.Context, request *gitalypb.UserMergeToRefRequest) (*gitalypb.UserMergeToRefResponse, error) {
	if err := validateUserMergeToRefRequest(request); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	repoPath, err := s.locator.GetPath(request.Repository)
	if err != nil {
		return nil, err
	}

	repo := s.localrepo(request.GetRepository())

	revision := git.Revision(request.Branch)
	if request.FirstParentRef != nil {
		revision = git.Revision(request.FirstParentRef)
	}

	oid, err := repo.ResolveRevision(ctx, revision)
	if err != nil {
		//nolint:stylecheck
		return nil, helper.ErrInvalidArgument(errors.New("Invalid merge source"))
	}

	sourceOID, err := repo.ResolveRevision(ctx, git.Revision(request.SourceSha))
	if err != nil {
		//nolint:stylecheck
		return nil, helper.ErrInvalidArgument(errors.New("Invalid merge source"))
	}

	authorDate, err := dateFromProto(request)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	// Resolve the current state of the target reference. We do not care whether it
	// exists or not, but what we do want to assert is that the target reference doesn't
	// change while we compute the merge commit as a small protection against races.
	var oldTargetOID git.ObjectID
	if targetRef, err := repo.GetReference(ctx, git.ReferenceName(request.TargetRef)); err == nil {
		if targetRef.IsSymbolic {
			return nil, helper.ErrFailedPreconditionf("target reference is symbolic: %q", request.TargetRef)
		}

		oid, err := git.ObjectHashSHA1.FromHex(targetRef.Target)
		if err != nil {
			return nil, helper.ErrInternalf("invalid target revision: %w", err)
		}

		oldTargetOID = oid
	} else if errors.Is(err, git.ErrReferenceNotFound) {
		oldTargetOID = git.ObjectHashSHA1.ZeroOID
	} else {
		return nil, helper.ErrInternalf("could not read target reference: %w", err)
	}

	// Now, we create the merge commit...
	merge, err := s.git2goExecutor.Merge(ctx, repo, git2go.MergeCommand{
		Repository:     repoPath,
		AuthorName:     string(request.User.Name),
		AuthorMail:     string(request.User.Email),
		AuthorDate:     authorDate,
		Message:        string(request.Message),
		Ours:           oid.String(),
		Theirs:         sourceOID.String(),
		AllowConflicts: request.AllowConflicts,
	})
	if err != nil {
		ctxlogrus.Extract(ctx).WithError(err).WithFields(
			logrus.Fields{
				"source_sha": sourceOID,
				"target_sha": oid,
				"target_ref": string(request.TargetRef),
			},
		).Error("unable to create merge commit")

		if errors.Is(err, git2go.ErrInvalidArgument) {
			return nil, helper.ErrInvalidArgument(err)
		}
		return nil, helper.ErrFailedPreconditionf("Failed to create merge commit for source_sha %s and target_sha %s at %s",
			sourceOID, oid, string(request.TargetRef))
	}

	mergeOID, err := git.ObjectHashSHA1.FromHex(merge.CommitID)
	if err != nil {
		return nil, helper.ErrInternalf("parsing merge commit SHA: %w", err)
	}

	// ... and move branch from target ref to the merge commit. The Ruby
	// implementation doesn't invoke hooks, so we don't either.
	if err := repo.UpdateRef(ctx, git.ReferenceName(request.TargetRef), mergeOID, oldTargetOID); err != nil {
		return nil, helper.ErrFailedPreconditionf("Could not update %s. Please refresh and try again", string(request.TargetRef))
	}

	return &gitalypb.UserMergeToRefResponse{
		CommitId: mergeOID.String(),
	}, nil
}
