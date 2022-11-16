package operations

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validateUserCreateBranchRequest(in *gitalypb.UserCreateBranchRequest) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}
	if len(in.BranchName) == 0 {
		return errors.New("Bad Request (empty branch name)") //nolint:stylecheck
	}
	if in.User == nil {
		return errors.New("empty user")
	}
	if len(in.StartPoint) == 0 {
		return errors.New("empty start point")
	}
	return nil
}

//nolint:revive // This is unintentionally missing documentation.
func (s *Server) UserCreateBranch(ctx context.Context, req *gitalypb.UserCreateBranchRequest) (*gitalypb.UserCreateBranchResponse, error) {
	if err := validateUserCreateBranchRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}
	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	// BEGIN TODO: Uncomment if StartPoint started behaving sensibly
	// like BranchName. See
	// https://gitlab.com/gitlab-org/gitaly/-/issues/3331
	//
	// startPointReference, err := s.localrepo(req.GetRepository()).GetReference(ctx, "refs/heads/"+string(req.StartPoint))
	// startPointCommit, err := log.GetCommit(ctx, req.Repository, startPointReference.Target)
	startPointCommit, err := quarantineRepo.ReadCommit(ctx, git.Revision(req.StartPoint))
	// END TODO
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "revspec '%s' not found", req.StartPoint)
	}

	startPointOID, err := git.ObjectHashSHA1.FromHex(startPointCommit.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not parse start point commit ID: %v", err)
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.BranchName))

	if err := s.updateReferenceWithHooks(ctx, req.GetRepository(), req.User, quarantineDir, referenceName, startPointOID, git.ObjectHashSHA1.ZeroOID); err != nil {
		var customHookErr updateref.CustomHookError

		if errors.As(err, &customHookErr) {
			detailedErr, err := helper.ErrWithDetails(
				// We explicitly don't include the custom hook error itself
				// in the returned error because that would also contain the
				// standard output or standard error in the error message.
				// It's thus needlessly verbose and duplicates information
				// we have available in the structured error anyway.
				helper.ErrPermissionDeniedf("creation denied by custom hooks"),
				&gitalypb.UserCreateBranchError{
					Error: &gitalypb.UserCreateBranchError_CustomHook{
						CustomHook: customHookErr.Proto(),
					},
				},
			)
			if err != nil {
				return nil, helper.ErrInternalf("error details: %w", err)
			}

			return nil, detailedErr
		}

		var updateRefError updateref.Error
		if errors.As(err, &updateRefError) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}

		return nil, err
	}

	return &gitalypb.UserCreateBranchResponse{
		Branch: &gitalypb.Branch{
			Name:         req.BranchName,
			TargetCommit: startPointCommit,
		},
	}, nil
}

func validateUserUpdateBranchGo(req *gitalypb.UserUpdateBranchRequest) error {
	if err := service.ValidateRepository(req.GetRepository()); err != nil {
		return err
	}

	if req.User == nil {
		return errors.New("empty user")
	}

	if len(req.BranchName) == 0 {
		return errors.New("empty branch name")
	}

	if len(req.Oldrev) == 0 {
		return errors.New("empty oldrev")
	}

	if len(req.Newrev) == 0 {
		return errors.New("empty newrev")
	}

	return nil
}

//nolint:revive // This is unintentionally missing documentation.
func (s *Server) UserUpdateBranch(ctx context.Context, req *gitalypb.UserUpdateBranchRequest) (*gitalypb.UserUpdateBranchResponse, error) {
	// Validate the request
	if err := validateUserUpdateBranchGo(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	newOID, err := git.ObjectHashSHA1.FromHex(string(req.Newrev))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not parse newrev: %v", err)
	}

	oldOID, err := git.ObjectHashSHA1.FromHex(string(req.Oldrev))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not parse oldrev: %v", err)
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.BranchName))

	quarantineDir, _, err := s.quarantinedRepo(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	if err := s.updateReferenceWithHooks(ctx, req.GetRepository(), req.User, quarantineDir, referenceName, newOID, oldOID); err != nil {
		var customHookErr updateref.CustomHookError
		if errors.As(err, &customHookErr) {
			return &gitalypb.UserUpdateBranchResponse{
				PreReceiveError: customHookErr.Error(),
			}, nil
		}

		// An oddball response for compatibility with the old
		// Ruby code. The "Could not update..."  message is
		// exactly like the default updateRefError, except we
		// say "branch-name", not
		// "refs/heads/branch-name". See the
		// "Gitlab::Git::CommitError" case in the Ruby code.
		return nil, status.Errorf(codes.FailedPrecondition, "Could not update %s. Please refresh and try again.", req.BranchName)
	}

	return &gitalypb.UserUpdateBranchResponse{}, nil
}

func validateUserDeleteBranchRequest(in *gitalypb.UserDeleteBranchRequest) error {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return err
	}
	if len(in.GetBranchName()) == 0 {
		return errors.New("bad request: empty branch name")
	}
	if in.GetUser() == nil {
		return errors.New("bad request: empty user")
	}
	return nil
}

// UserDeleteBranch force-deletes a single branch in the context of a specific user. It executes
// hooks and contacts Rails to verify that the user is indeed allowed to delete that branch.
func (s *Server) UserDeleteBranch(ctx context.Context, req *gitalypb.UserDeleteBranchRequest) (*gitalypb.UserDeleteBranchResponse, error) {
	if err := validateUserDeleteBranchRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}
	referenceName := git.NewReferenceNameFromBranchName(string(req.BranchName))

	var err error
	var referenceValue git.ObjectID

	if expectedOldOID := req.GetExpectedOldOid(); expectedOldOID != "" {
		referenceValue, err = s.localrepo(req.GetRepository()).ResolveRevision(ctx, git.Revision(expectedOldOID))
		if err != nil {
			return nil, helper.ErrFailedPreconditionf("object id: %s: %w", expectedOldOID, err)
		}
	} else {
		referenceValue, err = s.localrepo(req.GetRepository()).ResolveRevision(ctx, referenceName.Revision())
		if err != nil {
			return nil, helper.ErrFailedPreconditionf("branch not found: %q", req.BranchName)
		}
	}

	if err := s.updateReferenceWithHooks(ctx, req.Repository, req.User, nil, referenceName, git.ObjectHashSHA1.ZeroOID, referenceValue); err != nil {
		var notAllowedError hook.NotAllowedError
		var customHookErr updateref.CustomHookError
		var updateRefError updateref.Error

		if errors.As(err, &notAllowedError) {
			detailedErr, err := helper.ErrWithDetails(
				helper.ErrPermissionDeniedf("deletion denied by access checks: %w", err),
				&gitalypb.UserDeleteBranchError{
					Error: &gitalypb.UserDeleteBranchError_AccessCheck{
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
				return nil, helper.ErrInternalf("error details: %w", err)
			}

			return nil, detailedErr
		} else if errors.As(err, &customHookErr) {
			detailedErr, err := helper.ErrWithDetails(
				helper.ErrPermissionDeniedf("deletion denied by custom hooks: %w", err),
				&gitalypb.UserDeleteBranchError{
					Error: &gitalypb.UserDeleteBranchError_CustomHook{
						CustomHook: customHookErr.Proto(),
					},
				},
			)
			if err != nil {
				return nil, helper.ErrInternalf("error details: %w", err)
			}

			return nil, detailedErr
		} else if errors.As(err, &updateRefError) {
			detailedErr, err := helper.ErrWithDetails(
				helper.ErrFailedPreconditionf("reference update failed: %w", updateRefError),
				&gitalypb.UserDeleteBranchError{
					Error: &gitalypb.UserDeleteBranchError_ReferenceUpdate{
						ReferenceUpdate: &gitalypb.ReferenceUpdateError{
							ReferenceName: []byte(updateRefError.Reference.String()),
							OldOid:        updateRefError.OldOID.String(),
							NewOid:        updateRefError.NewOID.String(),
						},
					},
				},
			)
			if err != nil {
				return nil, helper.ErrInternalf("error details: %w", err)
			}

			return nil, detailedErr
		}

		return nil, helper.ErrInternalf("deleting reference: %w", err)
	}

	return &gitalypb.UserDeleteBranchResponse{}, nil
}
