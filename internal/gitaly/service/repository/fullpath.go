package repository

import (
	"context"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

const fullPathKey = "gitlab.fullpath"

// SetFullPath writes the provided path value into the repository's gitconfig under the
// "gitlab.fullpath" key.
func (s *server) SetFullPath(
	ctx context.Context,
	request *gitalypb.SetFullPathRequest,
) (*gitalypb.SetFullPathResponse, error) {
	repository := request.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if len(request.GetPath()) == 0 {
		return nil, helper.ErrInvalidArgumentf("no path provided")
	}

	repo := s.localrepo(repository)

	if err := repo.SetConfig(ctx, fullPathKey, request.GetPath(), s.txManager); err != nil {
		return nil, helper.ErrInternalf("setting config: %w", err)
	}

	return &gitalypb.SetFullPathResponse{}, nil
}

// FullPath reads the path from the repository's gitconfig under the
// "gitlab.fullpath" key.
func (s *server) FullPath(ctx context.Context, request *gitalypb.FullPathRequest) (*gitalypb.FullPathResponse, error) {
	repository := request.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	repo := s.localrepo(repository)
	var stdout strings.Builder
	err := repo.ExecAndWait(ctx, git.SubCmd{
		Name: "config",
		Args: []string{fullPathKey},
	}, git.WithStdout(&stdout))
	if err != nil {
		return nil, helper.ErrInternalf("fetch config: %w", err)
	}

	return &gitalypb.FullPathResponse{
		Path: strings.TrimSuffix(stdout.String(), "\n"),
	}, nil
}
