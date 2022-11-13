package operations

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/commit"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/tree"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const userUpdateSubmoduleName = "UserUpdateSubmodule"

//nolint:revive // This is unintentionally missing documentation.
func (s *Server) UserUpdateSubmodule(ctx context.Context, req *gitalypb.UserUpdateSubmoduleRequest) (*gitalypb.UserUpdateSubmoduleResponse, error) {
	if err := validateUserUpdateSubmoduleRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, userUpdateSubmoduleName+": %v", err)
	}

	return s.userUpdateSubmodule(ctx, req)
}

func validateUserUpdateSubmoduleRequest(req *gitalypb.UserUpdateSubmoduleRequest) error {
	if err := service.ValidateRepository(req.GetRepository()); err != nil {
		return err
	}

	if req.GetUser() == nil {
		return fmt.Errorf("empty User")
	}

	if req.GetCommitSha() == "" {
		return fmt.Errorf("empty CommitSha")
	}

	if match, err := regexp.MatchString(`\A[0-9a-f]{40}\z`, req.GetCommitSha()); !match || err != nil {
		return fmt.Errorf("invalid CommitSha")
	}

	if len(req.GetBranch()) == 0 {
		return fmt.Errorf("empty Branch")
	}

	if len(req.GetSubmodule()) == 0 {
		return fmt.Errorf("empty Submodule")
	}

	if len(req.GetCommitMessage()) == 0 {
		return fmt.Errorf("empty CommitMessage")
	}

	return nil
}

func (s *Server) updateSubmodule(ctx context.Context, quarantineRepo *localrepo.Repo, req *gitalypb.UserUpdateSubmoduleRequest) (string, error) {
	path := filepath.Dir(string(req.GetSubmodule()))
	if path == "." {
		path = ""
	}

	base := filepath.Base(string(req.GetSubmodule()))
	replaceWith := git.ObjectID(req.GetCommitSha())

	var submoduleFound bool

	// Start with the tree containing the submodule, and write a new tree
	// with the new submodule sha. Then, use that new tree id and go up the
	// paths until the repository root, rewriting the tree id each time.
	for {
		entries, err := tree.ListEntries(
			ctx,
			quarantineRepo,
			git.Revision("refs/heads/"+string(req.GetBranch())),
			&tree.ListEntriesConfig{
				RelativePath: path,
			},
		)
		if err != nil {
			return "", fmt.Errorf("error reading tree: %w", err)
		}

		var newEntries []*tree.Entry
		var newTreeID git.ObjectID

		for _, entry := range entries {
			if entry.Path != base {
				newEntries = append(newEntries, entry)
				continue
			}

			if string(entry.ObjectID) == req.GetCommitSha() {
				return "",
					//nolint:stylecheck
					fmt.Errorf(
						"The %s submodule is already at %s",
						req.GetSubmodule(),
						replaceWith,
					)
			}

			if entry.Path == filepath.Base(string(req.GetSubmodule())) {
				if entry.Type != tree.Submodule {
					return "", errors.New("submodule is not a commit")
				}
				submoduleFound = true
			}

			newEntries = append(newEntries, &tree.Entry{
				Mode:     entry.Mode,
				Type:     entry.Type,
				Path:     entry.Path,
				ObjectID: replaceWith,
			})
		}

		newTreeID, err = tree.Write(ctx, quarantineRepo, newEntries)
		if err != nil {
			return "", fmt.Errorf("write tree: %w", err)
		}
		replaceWith = newTreeID

		if path == "" {
			break
		}

		base = filepath.Base(path)
		path = filepath.Dir(path)
		if path == "." {
			path = ""
		}
	}

	if !submoduleFound {
		return "", errors.New("submodule not found")
	}

	currentBranchCommit, err := quarantineRepo.ResolveRevision(ctx, git.Revision(req.GetBranch()))
	if err != nil {
		return "", fmt.Errorf("resolving submodule branch: %w", err)
	}

	authorDate, err := dateFromProto(req)
	if err != nil {
		return "", helper.ErrInvalidArgument(err)
	}

	newCommitID, err := commit.Write(ctx, quarantineRepo, commit.Config{
		Parents:        []git.ObjectID{currentBranchCommit},
		AuthorDate:     authorDate,
		AuthorName:     string(req.GetUser().GetName()),
		AuthorEmail:    string(req.GetUser().GetEmail()),
		CommitterName:  string(req.GetUser().GetName()),
		CommitterEmail: string(req.GetUser().GetEmail()),
		CommitterDate:  authorDate,
		Message:        string(req.GetCommitMessage()),
		TreeID:         replaceWith,
	})
	if err != nil {
		return "", fmt.Errorf("creating commit %w", err)
	}

	return string(newCommitID), nil
}

func (s *Server) updateSubmoduleWithGit2Go(ctx context.Context, quarantineRepo *localrepo.Repo, req *gitalypb.UserUpdateSubmoduleRequest) (string, error) {
	repoPath, err := quarantineRepo.Path()
	if err != nil {
		return "", fmt.Errorf("%s: locate repo: %w", userUpdateSubmoduleName, err)
	}

	authorDate, err := dateFromProto(req)
	if err != nil {
		return "", helper.ErrInvalidArgument(err)
	}

	message := string(req.GetCommitMessage())
	if !strings.HasSuffix(message, "\n") {
		message += "\n"
	}

	result, err := s.git2goExecutor.Submodule(ctx, quarantineRepo, git2go.SubmoduleCommand{
		Repository: repoPath,
		AuthorMail: string(req.GetUser().GetEmail()),
		AuthorName: string(req.GetUser().GetName()),
		AuthorDate: authorDate,
		Branch:     string(req.GetBranch()),
		CommitSHA:  req.GetCommitSha(),
		Submodule:  string(req.GetSubmodule()),
		Message:    message,
	})
	if err != nil {
		return "", err
	}

	return result.CommitID, nil
}

func (s *Server) userUpdateSubmodule(ctx context.Context, req *gitalypb.UserUpdateSubmoduleRequest) (*gitalypb.UserUpdateSubmoduleResponse, error) {
	quarantineDir, quarantineRepo, err := s.quarantinedRepo(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	branches, err := quarantineRepo.GetBranches(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: get branches: %w", userUpdateSubmoduleName, err)
	}
	if len(branches) == 0 {
		return &gitalypb.UserUpdateSubmoduleResponse{
			CommitError: "Repository is empty",
		}, nil
	}

	referenceName := git.NewReferenceNameFromBranchName(string(req.GetBranch()))

	branchOID, err := quarantineRepo.ResolveRevision(ctx, referenceName.Revision())
	if err != nil {
		if errors.Is(err, git.ErrReferenceNotFound) {
			return nil, helper.ErrInvalidArgumentf("Cannot find branch")
		}
		return nil, fmt.Errorf("%s: get branch: %w", userUpdateSubmoduleName, err)
	}

	var commitID string

	if featureflag.SubmoduleInGit.IsEnabled(ctx) {
		commitID, err = s.updateSubmodule(ctx, quarantineRepo, req)
	} else {
		commitID, err = s.updateSubmoduleWithGit2Go(ctx, quarantineRepo, req)
	}

	if err != nil {
		errStr := strings.TrimPrefix(err.Error(), "submodule: ")
		errStr = strings.TrimSpace(errStr)

		var resp *gitalypb.UserUpdateSubmoduleResponse
		for _, legacyErr := range []string{
			git2go.LegacyErrPrefixInvalidBranch,
			git2go.LegacyErrPrefixInvalidSubmodulePath,
			git2go.LegacyErrPrefixFailedCommit,
		} {
			if strings.HasPrefix(errStr, legacyErr) {
				resp = &gitalypb.UserUpdateSubmoduleResponse{
					CommitError: legacyErr,
				}
				ctxlogrus.
					Extract(ctx).
					WithError(err).
					Error(userUpdateSubmoduleName + ": git2go subcommand failure")
				break
			}
		}
		if strings.Contains(errStr, "is already at") {
			resp = &gitalypb.UserUpdateSubmoduleResponse{
				CommitError: errStr,
			}
		}
		if resp != nil {
			return resp, nil
		}

		return nil, fmt.Errorf("%s: submodule subcommand: %w", userUpdateSubmoduleName, err)
	}

	commitOID, err := git.ObjectHashSHA1.FromHex(commitID)
	if err != nil {
		return nil, helper.ErrInvalidArgumentf("cannot parse commit ID: %w", err)
	}

	if err := s.updateReferenceWithHooks(
		ctx,
		req.GetRepository(),
		req.GetUser(),
		quarantineDir,
		referenceName,
		commitOID,
		branchOID,
	); err != nil {
		var customHookErr updateref.CustomHookError
		if errors.As(err, &customHookErr) {
			return &gitalypb.UserUpdateSubmoduleResponse{
				PreReceiveError: customHookErr.Error(),
			}, nil
		}

		var updateRefError updateref.Error
		if errors.As(err, &updateRefError) {
			return &gitalypb.UserUpdateSubmoduleResponse{
				CommitError: err.Error(),
			}, nil
		}

		return nil, err
	}

	return &gitalypb.UserUpdateSubmoduleResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      commitID,
			BranchCreated: false,
			RepoCreated:   false,
		},
	}, nil
}
