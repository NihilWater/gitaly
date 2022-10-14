package gitaly

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

func TestTransactionManager(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	// A clean repository is setup for each test. We build a repository ahead of the tests here once to
	// get deterministic commit IDs, relative path and object hash we can use to build the declarative
	// test cases.
	relativePath := gittest.NewRepositoryName(t)
	setupRepository := func(t *testing.T) (*localrepo.Repo, git.ObjectID, git.ObjectID, git.ObjectID) {
		t.Helper()

		cfg := testcfg.Build(t)

		repo, repoPath := gittest.CreateRepository(t, ctx, cfg, gittest.CreateRepositoryConfig{
			SkipCreationViaService: true,
			RelativePath:           relativePath,
		})

		rootCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())
		secondCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(rootCommitOID))
		thirdCommitOID := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents(secondCommitOID))

		cmdFactory, clean, err := git.NewExecCommandFactory(cfg)
		require.NoError(t, err)
		t.Cleanup(clean)

		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)

		localRepo := localrepo.New(
			config.NewLocator(cfg),
			cmdFactory,
			catfileCache,
			repo,
		)

		return localRepo, rootCommitOID, secondCommitOID, thirdCommitOID
	}

	// Collect commit OIDs and the object has so we can define the test cases with them.
	repo, rootCommitOID, secondCommitOID, thirdCommitOID := setupRepository(t)
	objectHash, err := repo.ObjectHash(ctx)
	require.NoError(t, err)

	type testHooks struct {
		// BeforeApplyLogEntry is called before a log entry is applied to the repository.
		BeforeApplyLogEntry hookFunc
		// BeforeAppendLogEntry is called before a log entry is appended to the log.
		BeforeAppendLogEntry hookFunc
		// WaitForTransactionsWhenStopping waits for a in-flight to finish before returning
		// from Run.
		WaitForTransactionsWhenStopping bool
	}

	// Step defines a single execution step in a test. Each test case can define multiple steps to setup exercise
	// more complex behavior and to assert the state after each step.
	type steps []struct {
		// StopManager stops the manager in the beginning of the step.
		StopManager bool
		// StartManager can be used to start the manager again after stopping it.
		StartManager bool
		// Context is the context to use for the Propose call of the step.
		Context context.Context
		// Transaction is the transaction that is proposed in this step.
		Transaction Transaction
		// Hooks contains the hook functions that are configured on the TransactionManager. These allow
		// for better synchronization.
		Hooks testHooks
		// ExpectedRunError is the expected error to be returned from Run from this step.
		ExpectedRunError bool
		// ExpectedError is the error that is expected to be returned when proposing the transaction in this step.
		ExpectedError error
		// ExpectedReferences is the expected state of references at the end of this step.
		ExpectedReferences []git.Reference
		// ExpectedDatabase is the expected state of the database at the end of this step.
		ExpectedDatabase DatabaseState
	}

	type testCase struct {
		desc  string
		steps steps
	}

	// prepareErr constructs an error as it would be returned from a verification failure.
	// Used to do error equality assertions until a better error interface is set.
	prepareErr := func(err error) error {
		return fmt.Errorf("verify references: %w",
			fmt.Errorf("verify references with git: %w",
				fmt.Errorf("prepare reference transaction: %w",
					fmt.Errorf("prepare: %w", err),
				),
			),
		)
	}

	testCases := []testCase{
		{
			desc: "invalid reference aborts the entire transaction",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceVerificationStrategy: ReferenceVerificationStrategySkip,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":    {NewOID: rootCommitOID},
							"refs/heads/../main": {NewOID: rootCommitOID},
						},
					},
					ExpectedError: InvalidReferenceFormatError{ReferenceName: "refs/heads/../main"},
				},
			},
		},
		{
			desc: "continues processing after aborting due to an invalid reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceVerificationStrategy: ReferenceVerificationStrategySkip,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/../main": {NewOID: rootCommitOID},
						},
					},
					ExpectedError: InvalidReferenceFormatError{ReferenceName: "refs/heads/../main"},
				},
				{
					Transaction: Transaction{
						ReferenceVerificationStrategy: ReferenceVerificationStrategySkip,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "create reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "create a file-directory reference conflict different transaction",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/parent": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/parent", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/parent"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedError:      prepareErr(&updateref.ErrAlreadyLocked{Ref: "refs/heads/parent/child"}),
					ExpectedReferences: []git.Reference{{Name: "refs/heads/parent", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/parent"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "create a file-directory reference conflict in same transaction",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/parent":       {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedError: prepareErr(&updateref.ErrAlreadyLocked{Ref: "refs/heads/parent"}),
				},
			},
		},
		{
			desc: "file-directory conflict aborts the transaction with verification failures skipped",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceVerificationStrategy: ReferenceVerificationStrategySkipFailures,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":         {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/parent":       {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/parent/child": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedError: prepareErr(&updateref.ErrAlreadyLocked{Ref: "refs/heads/parent"}),
				},
			},
		},
		{
			desc: "file-directory conflict aborts the transaction with verification skipped",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceVerificationStrategy: ReferenceVerificationStrategySkip,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/parent":       {NewOID: rootCommitOID},
							"refs/heads/parent/child": {NewOID: rootCommitOID},
						},
					},
					ExpectedError: prepareErr(&updateref.ErrAlreadyLocked{Ref: "refs/heads/parent"}),
				},
			},
		},
		{
			desc: "create reference ignoring verification failure",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceVerificationStrategy: ReferenceVerificationStrategySkipFailures,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: secondCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 2},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
						string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/non-conflicting"),
									NewOid:        []byte(secondCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "create reference that already exists",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
						},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   objectHash.ZeroOID,
						ActualOID:     rootCommitOID,
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "create reference no-op",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   objectHash.ZeroOID,
						ActualOID:     rootCommitOID,
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "update reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 2},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
						string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(secondCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "force update reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceVerificationStrategy: ReferenceVerificationStrategySkip,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {NewOID: secondCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 2},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
						string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(secondCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "update reference ignoring verification failures",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
								{
									ReferenceName: []byte("refs/heads/non-conflicting"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceVerificationStrategy: ReferenceVerificationStrategySkipFailures,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: secondCommitOID, NewOID: thirdCommitOID},
							"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: thirdCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: thirdCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 2},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
								{
									ReferenceName: []byte("refs/heads/non-conflicting"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
						string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/non-conflicting"),
									NewOid:        []byte(thirdCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "update reference with incorrect old tip",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
								{
									ReferenceName: []byte("refs/heads/non-conflicting"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: secondCommitOID, NewOID: thirdCommitOID},
							"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: thirdCommitOID},
						},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   secondCommitOID,
						ActualOID:     rootCommitOID,
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
								{
									ReferenceName: []byte("refs/heads/non-conflicting"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "update non-existent reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: secondCommitOID, NewOID: thirdCommitOID},
						},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   secondCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
				},
			},
		},
		{
			desc: "update reference no-op",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 2},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
						string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "delete reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 2},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
						string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(objectHash.ZeroOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "force delete reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceVerificationStrategy: ReferenceVerificationStrategySkip,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {NewOID: objectHash.ZeroOID},
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 2},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
						string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(objectHash.ZeroOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "delete reference ignoring verification failures",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
								{
									ReferenceName: []byte("refs/heads/non-conflicting"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceVerificationStrategy: ReferenceVerificationStrategySkipFailures,
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: secondCommitOID, NewOID: objectHash.ZeroOID},
							"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 2},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
								{
									ReferenceName: []byte("refs/heads/non-conflicting"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
						string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/non-conflicting"),
									NewOid:        []byte(objectHash.ZeroOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "delete reference with incorrect old tip",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							"refs/heads/non-conflicting": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
								{
									ReferenceName: []byte("refs/heads/non-conflicting"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main":            {OldOID: secondCommitOID, NewOID: objectHash.ZeroOID},
							"refs/heads/non-conflicting": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
						},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   secondCommitOID,
						ActualOID:     rootCommitOID,
					},
					ExpectedReferences: []git.Reference{
						{Name: "refs/heads/main", Target: rootCommitOID.String()},
						{Name: "refs/heads/non-conflicting", Target: rootCommitOID.String()},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
								{
									ReferenceName: []byte("refs/heads/non-conflicting"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "delete non-existent reference",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: objectHash.ZeroOID},
						},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   rootCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
				},
			},
		},
		{
			desc: "delete reference no-op",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: objectHash.ZeroOID},
						},
					},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(objectHash.ZeroOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "continues processing after reference verification failure",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
						},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   rootCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(secondCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "continues processing after a restart",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					StopManager:  true,
					StartManager: true,
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 2},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
						string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(secondCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "continues processing after restarting after a reference verification failure",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
						},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   rootCommitOID,
						ActualOID:     objectHash.ZeroOID,
					},
				},
				{
					StopManager:  true,
					StartManager: true,
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: secondCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(secondCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "recovers from the write-ahead log on start up",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.stopManager()
						},
					},
					ExpectedError:    ErrTransactionProcessingStopped,
					ExpectedRunError: true,
					ExpectedDatabase: DatabaseState{
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: rootCommitOID, NewOID: secondCommitOID},
						},
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: secondCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 2},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
						string(keyLogEntry(getRepositoryID(repo), 2)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(secondCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "reference verification fails after recovering logged writes",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.stopManager()
						},
					},
					ExpectedError:    ErrTransactionProcessingStopped,
					ExpectedRunError: true,
					ExpectedDatabase: DatabaseState{
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: secondCommitOID, NewOID: rootCommitOID},
						},
					},
					ExpectedError: ReferenceVerificationError{
						ReferenceName: "refs/heads/main",
						ExpectedOID:   secondCommitOID,
						ActualOID:     rootCommitOID,
					},
					ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
					ExpectedDatabase: DatabaseState{
						string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
			},
		},
		{
			desc: "propose returns if context is canceled before admission",
			steps: steps{
				{
					Context: func() context.Context {
						ctx, cancel := context.WithCancel(ctx)
						cancel()
						return ctx
					}(),
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedError: context.Canceled,
				},
			},
		},
		{
			desc: "propose returns if transaction processing stops before admission",
			steps: steps{
				{
					StopManager: true,
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					ExpectedError: ErrTransactionProcessingStopped,
				},
			},
		},
		func() testCase {
			ctx, cancel := context.WithCancel(ctx)
			return testCase{
				desc: "propose returns if context is canceled after admission",
				steps: steps{
					{
						Context: ctx,
						Transaction: Transaction{
							ReferenceUpdates: ReferenceUpdates{
								"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
							},
						},
						Hooks: testHooks{
							BeforeApplyLogEntry: func(hookCtx hookContext) {
								// Cancel the context used in Propose
								cancel()
							},
						},
						ExpectedError:      context.Canceled,
						ExpectedReferences: []git.Reference{{Name: "refs/heads/main", Target: rootCommitOID.String()}},
						ExpectedDatabase: DatabaseState{
							string(keyAppliedLogIndex(getRepositoryID(repo))): &gitalypb.LogIndex{LogIndex: 1},
							string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
								ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
									{
										ReferenceName: []byte("refs/heads/main"),
										NewOid:        []byte(rootCommitOID),
									},
								},
							},
						},
					},
				},
			}
		}(),
		{
			desc: "propose returns if transaction processing stops before transaction acceptance",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					Hooks: testHooks{
						BeforeAppendLogEntry: func(hookContext hookContext) { hookContext.stopManager() },
						// This ensures we are testing the context cancellation errors being unwrapped properly
						// to an ErrTransactionProcessingStopped instead of hitting the general case when
						// runDone is closed.
						WaitForTransactionsWhenStopping: true,
					},
					ExpectedError: ErrTransactionProcessingStopped,
				},
			},
		},
		{
			desc: "propose returns if transaction processing stops after transaction acceptance",
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceUpdates: ReferenceUpdates{
							"refs/heads/main": {OldOID: objectHash.ZeroOID, NewOID: rootCommitOID},
						},
					},
					Hooks: testHooks{
						BeforeApplyLogEntry: func(hookCtx hookContext) {
							hookCtx.stopManager()
						},
					},
					ExpectedError:    ErrTransactionProcessingStopped,
					ExpectedRunError: true,
					ExpectedDatabase: DatabaseState{
						string(keyLogEntry(getRepositoryID(repo), 1)): &gitalypb.LogEntry{
							ReferenceUpdates: []*gitalypb.LogEntry_ReferenceUpdate{
								{
									ReferenceName: []byte("refs/heads/main"),
									NewOid:        []byte(rootCommitOID),
								},
							},
						},
					},
				},
			},
		},
	}

	// Generate test cases for the reference format rules according to https://git-scm.com/docs/git-check-ref-format.
	// This is to ensure the references are correctly validated prior to logging so they are guaranteed to apply later.
	for _, tc := range []struct {
		desc             string
		referenceName    git.ReferenceName
		invalidReference git.ReferenceName
	}{
		// 1. They can include slash / for hierarchical (directory) grouping, but no slash-separated
		// component can begin with a dot . or end with the sequence .lock.
		{"starting with a period", ".refs/heads/main", ""},
		{"subcomponent starting with a period", "refs/heads/.main", ""},
		{"ending in .lock", "refs/heads/main.lock", ""},
		{"subcomponent ending in .lock", "refs/heads/main.lock/main", ""},
		// 2. They must contain at least one /. This enforces the presence of a category like heads/,
		// tags/ etc. but the actual names are not restricted.
		{"without a /", "one-level", ""},
		{"with refs without a /", "refs", ""},
		// We restrict this further by requiring a 'refs/' prefix to ensure loose references only end up
		// in the 'refs/' folder.
		{"without refs/ prefix ", "nonrefs/main", ""},
		// 3. They cannot have two consecutive dots .. anywhere.
		{"containing two consecutive dots", "refs/heads/../main", ""},
		// 4. They cannot have ASCII control characters (i.e. bytes whose values are lower than \040,
		//    or \177 DEL), space, tilde ~, caret ^, or colon : anywhere.
		{`containing ASCII control character lower than \040`, "refs/heads/ma\040in", ""},
		{"containing DEL", "refs/heads/ma\177in", "refs/heads/ma?in"},
		{"containing space", "refs/heads/ma in", ""},
		{"containing ~", "refs/heads/ma~in", ""},
		{"containing ^", "refs/heads/ma^in", ""},
		{"containing :", "refs/heads/ma:in", ""},
		// 5. They cannot have question-mark ?, asterisk *, or open bracket [ anywhere.
		{"containing ?", "refs/heads/ma?in", ""},
		{"containing *", "refs/heads/ma*in", ""},
		{"containing [", "refs/heads/ma[in", ""},
		// 6. They cannot begin or end with a slash / or contain multiple consecutive slashes
		{"begins with /", "/refs/heads/main", ""},
		{"ends with /", "refs/heads/main/", ""},
		{"contains consecutive /", "refs/heads//main", ""},
		// 7. They cannot end with a dot.
		{"ending in a dot", "refs/heads/main.", ""},
		// 8. They cannot contain a sequence @{.
		{"invalid reference contains @{", "refs/heads/m@{n", ""},
		// 9. They cannot be the single character @.
		{"is a single character @", "@", ""},
		// 10. They cannot contain a \.
		{`containing \`, `refs/heads\main`, `refs/heads\\main`},
	} {

		invalidReference := tc.invalidReference
		if invalidReference == "" {
			invalidReference = tc.referenceName
		}

		testCases = append(testCases, testCase{
			desc: fmt.Sprintf("invalid reference %s", tc.desc),
			steps: steps{
				{
					Transaction: Transaction{
						ReferenceVerificationStrategy: ReferenceVerificationStrategySkip,
						ReferenceUpdates: ReferenceUpdates{
							tc.referenceName: {NewOID: rootCommitOID},
						},
					},
					ExpectedError: InvalidReferenceFormatError{ReferenceName: invalidReference},
				},
			},
		})
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			// Setup the repository with the exact same state as what was used to build the test cases.
			repository, _, _, _ := setupRepository(t)

			database, err := OpenDatabase(t.TempDir())
			require.NoError(t, err)
			defer testhelper.MustClose(t, database)

			var (
				// managerRunning tracks whether the manager is running or stopped.
				managerRunning bool
				// transactionManager is the current TransactionManager instance.
				transactionManager *TransactionManager
				// managerErr is used for synchronizing manager stopping and returning
				// the error from Run.
				managerErr chan error
				// inflightTransactions tracks the number of on going propose calls. It is used to synchronize
				// the database hooks with propose calls.
				inflightTransactions sync.WaitGroup
			)

			// stopManager stops the manager. It waits until the manager's Run method has exited.
			stopManager := func() {
				t.Helper()

				transactionManager.Stop()
				managerRunning, err = checkManagerError(t, managerErr, transactionManager)
				require.NoError(t, err)
			}

			// startManager starts fresh manager and applies hooks into it.
			startManager := func(testHooks testHooks) {
				t.Helper()

				require.False(t, managerRunning, "manager started while it was already running")
				managerRunning = true
				managerErr = make(chan error)

				transactionManager = NewTransactionManager(database, repository)
				installHooks(t, transactionManager, database, repository, hooks{
					beforeResolveRevision: testHooks.BeforeAppendLogEntry,
					beforeReadLogEntry:    testHooks.BeforeApplyLogEntry,
					beforeDeferredStop: func(hookContext) {
						if testHooks.WaitForTransactionsWhenStopping {
							inflightTransactions.Wait()
						}
					},
				})

				go func() { managerErr <- transactionManager.Run() }()
			}

			// Stop the manager if it is running at the end of the test.
			defer stopManager()
			for _, step := range tc.steps {
				// Ensure every step starts with the manager running.
				if !managerRunning {
					startManager(step.Hooks)
				}

				if step.StopManager {
					require.True(t, managerRunning, "manager stopped while it was already stopped")
					stopManager()
				}

				if step.StartManager {
					startManager(step.Hooks)
				}

				func() {
					inflightTransactions.Add(1)
					defer inflightTransactions.Done()

					proposeCtx := ctx
					if step.Context != nil {
						proposeCtx = step.Context
					}

					require.Equal(t, step.ExpectedError, transactionManager.Propose(proposeCtx, step.Transaction))
				}()

				if managerRunning, err = checkManagerError(t, managerErr, transactionManager); step.ExpectedRunError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}

				RequireReferences(t, ctx, repository, step.ExpectedReferences)
				RequireDatabase(t, ctx, database, step.ExpectedDatabase)
			}
		})
	}
}

func checkManagerError(t *testing.T, managerErr chan error, mgr *TransactionManager) (bool, error) {
	t.Helper()

	select {
	case err, ok := <-managerErr:
		if ok {
			close(managerErr)
		}

		// managerErr returns the possible error if manager has already stopped.
		return false, err
	case mgr.admissionQueue <- transactionFuture{
		transaction: Transaction{ReferenceUpdates: ReferenceUpdates{"sentinel": {}}},
		result:      make(resultFuture, 1),
	}:
		// If the error channel doesn't receive, we don't know whether it is because the manager is still running
		// or we are still waiting for it to return. We test whether the manager is running or not here by queueing a
		// a proposal that will error. If the manager processes it, we know it is still running.
		return true, nil
	}
}
