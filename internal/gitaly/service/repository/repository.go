package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Deprecated
func (s *server) Exists(ctx context.Context, in *gitalypb.RepositoryExistsRequest) (*gitalypb.RepositoryExistsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "this rpc is not implemented")
}

func (s *server) RepositoryExists(ctx context.Context, in *gitalypb.RepositoryExistsRequest) (*gitalypb.RepositoryExistsResponse, error) {
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}
	path, err := s.locator.GetPath(in.Repository)
	if err != nil {
		return nil, err
	}

	return &gitalypb.RepositoryExistsResponse{Exists: storage.IsGitDirectory(path)}, nil
}

func (s *server) HasLocalBranches(ctx context.Context, in *gitalypb.HasLocalBranchesRequest) (*gitalypb.HasLocalBranchesResponse, error) {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}
	hasBranches, err := s.localrepo(repository).HasBranches(ctx)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.HasLocalBranchesResponse{Value: hasBranches}, nil
}
