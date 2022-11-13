package repository

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"regexp"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/tree"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

const (
	surroundContext = "2"

	// searchFilesFilterMaxLength controls the maximum length of the regular
	// expression to thwart excessive resource usage when filtering
	searchFilesFilterMaxLength = 1000
)

var contentDelimiter = []byte("--\n")

func (s *server) SearchFilesByContent(req *gitalypb.SearchFilesByContentRequest, stream gitalypb.RepositoryService_SearchFilesByContentServer) error {
	if err := validateSearchFilesRequest(req); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	ctx := stream.Context()
	cmd, err := s.gitCmdFactory.New(ctx, req.GetRepository(),
		git.SubCmd{Name: "grep", Flags: []git.Option{
			git.Flag{Name: "--ignore-case"},
			git.Flag{Name: "-I"},
			git.Flag{Name: "--line-number"},
			git.Flag{Name: "--null"},
			git.ValueFlag{Name: "--before-context", Value: surroundContext},
			git.ValueFlag{Name: "--after-context", Value: surroundContext},
			git.Flag{Name: "--perl-regexp"},
			git.Flag{Name: "-e"},
		}, Args: []string{req.GetQuery(), string(req.GetRef())}})
	if err != nil {
		return helper.ErrInternalf("SearchFilesByContent: cmd start failed: %v", err)
	}

	if err = sendSearchFilesResultChunked(cmd, stream); err != nil {
		return helper.ErrInternalf("SearchFilesByContent: sending chunked response failed: %v", err)
	}

	return nil
}

func sendMatchInChunks(buf []byte, stream gitalypb.RepositoryService_SearchFilesByContentServer) error {
	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.SearchFilesByContentResponse{MatchData: p})
	})

	if _, err := io.Copy(sw, bytes.NewReader(buf)); err != nil {
		return err
	}

	return stream.Send(&gitalypb.SearchFilesByContentResponse{EndOfMatch: true})
}

func sendSearchFilesResultChunked(cmd *command.Command, stream gitalypb.RepositoryService_SearchFilesByContentServer) error {
	var buf []byte
	scanner := bufio.NewScanner(cmd)

	for scanner.Scan() {
		// Intentionally avoid scanner.Bytes() because that returns a []byte that
		// becomes invalid on the next loop iteration, and we want to hold on to
		// the contents of the current line for a while. Scanner.Text() is a
		// string and hence immutable.
		line := scanner.Text() + "\n"

		if line == string(contentDelimiter) {
			if err := sendMatchInChunks(buf, stream); err != nil {
				return err
			}

			buf = nil
			continue
		}

		buf = append(buf, line...)
	}

	if len(buf) > 0 {
		return sendMatchInChunks(buf, stream)
	}

	return nil
}

func (s *server) SearchFilesByName(req *gitalypb.SearchFilesByNameRequest, stream gitalypb.RepositoryService_SearchFilesByNameServer) error {
	if err := validateSearchFilesRequest(req); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	var filter *regexp.Regexp
	if req.GetFilter() != "" {
		if len(req.GetFilter()) > searchFilesFilterMaxLength {
			return helper.ErrInvalidArgumentf("SearchFilesByName: filter exceeds maximum length")
		}
		var err error
		filter, err = regexp.Compile(req.GetFilter())
		if err != nil {
			return helper.ErrInvalidArgumentf("SearchFilesByName: filter did not compile: %v", err)
		}
	}

	ctx := stream.Context()
	cmd, err := s.gitCmdFactory.New(
		ctx,
		req.GetRepository(),
		git.SubCmd{Name: "ls-tree", Flags: []git.Option{
			git.Flag{Name: "--full-tree"},
			git.Flag{Name: "--name-status"},
			git.Flag{Name: "-r"},
			// We use -z to force NULL byte termination here to prevent git from
			// quoting and escaping unusual file names. Lstree parser would be a
			// more ideal solution. Unfortunately, it supports parsing full
			// output while we are interested in the filenames only.
			git.Flag{Name: "-z"},
		}, Args: []string{string(req.GetRef()), req.GetQuery()}})
	if err != nil {
		return helper.ErrInternalf("SearchFilesByName: cmd start failed: %v", err)
	}

	files, err := parseLsTree(cmd, filter, int(req.GetOffset()), int(req.GetLimit()))
	if err != nil {
		return err
	}

	return stream.Send(&gitalypb.SearchFilesByNameResponse{Files: files})
}

type searchFilesRequest interface {
	GetRepository() *gitalypb.Repository
	GetRef() []byte
	GetQuery() string
}

func validateSearchFilesRequest(req searchFilesRequest) error {
	if err := service.ValidateRepository(req.GetRepository()); err != nil {
		return err
	}

	if len(req.GetQuery()) == 0 {
		return errors.New("no query given")
	}

	if len(req.GetRef()) == 0 {
		return errors.New("no ref given")
	}

	if bytes.HasPrefix(req.GetRef(), []byte("-")) {
		return errors.New("invalid ref argument")
	}

	return nil
}

func parseLsTree(cmd *command.Command, filter *regexp.Regexp, offset int, limit int) ([][]byte, error) {
	var files [][]byte
	var index int
	parser := tree.NewParser(cmd, git.ObjectHashSHA1)

	for {
		path, err := parser.NextEntryPath()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if filter != nil && !filter.Match(path) {
			continue
		}

		index++
		if index > offset {
			files = append(files, path)
		}
		if limit > 0 && len(files) >= limit {
			break
		}
	}

	return files, nil
}
