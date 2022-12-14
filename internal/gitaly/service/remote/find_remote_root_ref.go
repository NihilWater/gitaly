package remote

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

const headPrefix = "HEAD branch: "

func (s *server) findRemoteRootRefCmd(ctx context.Context, request *gitalypb.FindRemoteRootRefRequest) (*command.Command, error) {
	remoteURL := request.GetRemoteUrl()
	var config []git.ConfigPair

	if resolvedAddress := request.GetResolvedAddress(); resolvedAddress != "" {
		modifiedURL, resolveConfig, err := git.GetURLAndResolveConfig(remoteURL, resolvedAddress)
		if err != nil {
			return nil, helper.ErrInvalidArgumentf("couldn't get curloptResolve config: %w", err)
		}

		remoteURL = modifiedURL
		config = append(config, resolveConfig...)
	}

	config = append(config, git.ConfigPair{Key: "remote.inmemory.url", Value: remoteURL})

	if authHeader := request.GetHttpAuthorizationHeader(); authHeader != "" {
		config = append(config, git.ConfigPair{
			Key:   fmt.Sprintf("http.%s.extraHeader", request.RemoteUrl),
			Value: "Authorization: " + authHeader,
		})
	}
	//nolint:staticcheck
	if host := request.GetHttpHost(); host != "" {
		config = append(config, git.ConfigPair{
			Key:   fmt.Sprintf("http.%s.extraHeader", request.RemoteUrl),
			Value: "Host: " + host,
		})
	}

	return s.gitCmdFactory.New(ctx, request.Repository,
		git.SubSubCmd{
			Name:   "remote",
			Action: "show",
			Args:   []string{"inmemory"},
		},
		git.WithRefTxHook(request.Repository),
		git.WithConfigEnv(config...),
	)
}

func (s *server) findRemoteRootRef(ctx context.Context, request *gitalypb.FindRemoteRootRefRequest) (string, error) {
	cmd, err := s.findRemoteRootRefCmd(ctx, request)
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, headPrefix) {
			rootRef := strings.TrimPrefix(line, headPrefix)
			if rootRef == "(unknown)" {
				return "", helper.ErrNotFoundf("no remote HEAD found")
			}
			return rootRef, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", err
	}

	return "", helper.ErrNotFoundf("couldn't query the remote HEAD")
}

// FindRemoteRootRef queries the remote to determine its HEAD
func (s *server) FindRemoteRootRef(ctx context.Context, in *gitalypb.FindRemoteRootRefRequest) (*gitalypb.FindRemoteRootRefResponse, error) {
	if in.GetRemoteUrl() == "" {
		return nil, helper.ErrInvalidArgumentf("missing remote URL")
	}
	if err := service.ValidateRepository(in.GetRepository()); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	ref, err := s.findRemoteRootRef(ctx, in)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.FindRemoteRootRefResponse{Ref: ref}, nil
}
