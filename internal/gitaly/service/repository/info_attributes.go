package repository

import (
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) GetInfoAttributes(in *gitalypb.GetInfoAttributesRequest, stream gitalypb.RepositoryService_GetInfoAttributesServer) error {
	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return helper.ErrInvalidArgument(err)
	}
	repoPath, err := s.locator.GetRepoPath(repository)
	if err != nil {
		return err
	}

	attrFile := filepath.Join(repoPath, "info", "attributes")
	f, err := os.Open(attrFile)
	if err != nil {
		if os.IsNotExist(err) {
			return stream.Send(&gitalypb.GetInfoAttributesResponse{})
		}

		return status.Errorf(codes.Internal, "GetInfoAttributes failure to read info attributes: %v", err)
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetInfoAttributesResponse{
			Attributes: p,
		})
	})

	_, err = io.Copy(sw, f)
	return err
}
