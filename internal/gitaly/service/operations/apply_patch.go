package operations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v15/streamio"
)

var errNoDefaultBranch = errors.New("no default branch")

type gitError struct {
	// ErrMsg error message from 'git' executable if any.
	ErrMsg string
	// Err is an error that happened during rebase process.
	Err error
}

func (er gitError) Error() string {
	return er.ErrMsg + ": " + er.Err.Error()
}

//nolint:revive // This is unintentionally missing documentation.
func (s *Server) UserApplyPatch(stream gitalypb.OperationService_UserApplyPatchServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	header := firstRequest.GetHeader()
	if header == nil {
		return helper.ErrInvalidArgumentf("empty UserApplyPatch_Header")
	}

	if err := validateUserApplyPatchHeader(header); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := s.userApplyPatch(stream.Context(), header, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *Server) userApplyPatch(ctx context.Context, header *gitalypb.UserApplyPatchRequest_Header, stream gitalypb.OperationService_UserApplyPatchServer) error {
	path, err := s.locator.GetRepoPath(header.Repository)
	if err != nil {
		return err
	}

	branchCreated := false
	targetBranch := git.NewReferenceNameFromBranchName(string(header.TargetBranch))

	repo := s.localrepo(header.Repository)
	parentCommitID, err := repo.ResolveRevision(ctx, targetBranch.Revision()+"^{commit}")
	if err != nil {
		if !errors.Is(err, git.ErrReferenceNotFound) {
			return fmt.Errorf("resolve target branch: %w", err)
		}

		defaultBranch, err := repo.GetDefaultBranch(ctx)
		if err != nil {
			return fmt.Errorf("default branch name: %w", err)
		} else if len(defaultBranch) == 0 {
			return errNoDefaultBranch
		}

		branchCreated = true
		parentCommitID, err = repo.ResolveRevision(ctx, defaultBranch.Revision()+"^{commit}")
		if err != nil {
			return fmt.Errorf("resolve default branch commit: %w", err)
		}
	}

	committerTime := time.Now()
	if header.Timestamp != nil {
		committerTime, err = dateFromProto(header)
		if err != nil {
			return helper.ErrInvalidArgument(err)
		}
	}

	worktreePath := newWorktreePath(path, "am-")
	if err := s.addWorktree(ctx, repo, worktreePath, parentCommitID.String()); err != nil {
		return fmt.Errorf("add worktree: %w", err)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(helper.SuppressCancellation(ctx), 30*time.Second)
		defer cancel()

		worktreeName := filepath.Base(worktreePath)
		if err := s.removeWorktree(ctx, header.Repository, worktreeName); err != nil {
			ctxlogrus.Extract(ctx).WithField("worktree_name", worktreeName).WithError(err).Error("failed to remove worktree")
		}
	}()

	var stdout, stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx,
		git.SubCmd{
			Name: "am",
			Flags: []git.Option{
				git.Flag{Name: "--quiet"},
				git.Flag{Name: "--3way"},
			},
		},
		git.WithEnv(
			"GIT_COMMITTER_NAME="+string(header.GetUser().Name),
			"GIT_COMMITTER_EMAIL="+string(header.GetUser().Email),
			fmt.Sprintf("GIT_COMMITTER_DATE=%d %s", committerTime.Unix(), committerTime.Format("-0700")),
		),
		git.WithStdin(streamio.NewReader(func() ([]byte, error) {
			req, err := stream.Recv()
			return req.GetPatches(), err
		})),
		git.WithStdout(&stdout),
		git.WithStderr(&stderr),
		git.WithRefTxHook(header.Repository),
		git.WithWorktree(worktreePath),
	); err != nil {
		// The Ruby implementation doesn't include stderr in errors, which makes
		// it difficult to determine the cause of an error. This special cases the
		// user facing patching error which is returned usually to maintain test
		// compatibility but returns the error and stderr otherwise. Once the Ruby
		// implementation is removed, this should probably be dropped.
		if bytes.HasPrefix(stdout.Bytes(), []byte("Patch failed at")) {
			return helper.ErrFailedPreconditionf(stdout.String())
		}

		return fmt.Errorf("apply patch: %w, stderr: %q", err, &stderr)
	}

	var revParseStdout, revParseStderr bytes.Buffer
	if err := repo.ExecAndWait(ctx,
		git.SubCmd{
			Name: "rev-parse",
			Flags: []git.Option{
				git.Flag{Name: "--quiet"},
				git.Flag{Name: "--verify"},
			},
			Args: []string{"HEAD^{commit}"},
		},
		git.WithStdout(&revParseStdout),
		git.WithStderr(&revParseStderr),
		git.WithWorktree(worktreePath),
	); err != nil {
		return fmt.Errorf("get patched commit: %w", gitError{ErrMsg: revParseStderr.String(), Err: err})
	}

	patchedCommit, err := git.ObjectHashSHA1.FromHex(text.ChompBytes(revParseStdout.Bytes()))
	if err != nil {
		return fmt.Errorf("parse patched commit oid: %w", err)
	}

	currentCommit := parentCommitID
	if branchCreated {
		currentCommit = git.ObjectHashSHA1.ZeroOID
	}

	if err := s.updateReferenceWithHooks(ctx, header.Repository, header.User, nil, targetBranch, patchedCommit, currentCommit); err != nil {
		return fmt.Errorf("update reference: %w", err)
	}

	if err := stream.SendAndClose(&gitalypb.UserApplyPatchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      patchedCommit.String(),
			BranchCreated: branchCreated,
		},
	}); err != nil {
		return fmt.Errorf("send and close: %w", err)
	}

	return nil
}

func validateUserApplyPatchHeader(header *gitalypb.UserApplyPatchRequest_Header) error {
	if err := service.ValidateRepository(header.GetRepository()); err != nil {
		return err
	}

	if header.GetUser() == nil {
		return errors.New("missing User")
	}

	if len(header.GetTargetBranch()) == 0 {
		return errors.New("missing Branch")
	}

	return nil
}

func (s *Server) addWorktree(ctx context.Context, repo *localrepo.Repo, worktreePath string, committish string) error {
	args := []string{worktreePath}
	flags := []git.Option{git.Flag{Name: "--detach"}}
	if committish != "" {
		args = append(args, committish)
	} else {
		flags = append(flags, git.Flag{Name: "--no-checkout"})
	}

	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx, git.SubSubCmd{
		Name:   "worktree",
		Action: "add",
		Flags:  flags,
		Args:   args,
	}, git.WithStderr(&stderr), git.WithRefTxHook(repo)); err != nil {
		return fmt.Errorf("adding worktree: %w", gitError{ErrMsg: stderr.String(), Err: err})
	}

	return nil
}

func (s *Server) removeWorktree(ctx context.Context, repo *gitalypb.Repository, worktreeName string) error {
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.SubSubCmd{
			Name:   "worktree",
			Action: "remove",
			Flags:  []git.Option{git.Flag{Name: "--force"}},
			Args:   []string{worktreeName},
		},
		git.WithRefTxHook(repo),
	)
	if err != nil {
		return fmt.Errorf("creation of 'worktree remove': %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("wait for 'worktree remove': %w", err)
	}

	return nil
}

func newWorktreePath(repoPath, prefix string) string {
	chars := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	rand.Shuffle(len(chars), func(i, j int) { chars[i], chars[j] = chars[j], chars[i] })
	return filepath.Join(repoPath, gitlabWorktreesSubDir, prefix+string(chars[:32]))
}
