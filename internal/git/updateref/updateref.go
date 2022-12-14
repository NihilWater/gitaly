package updateref

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"regexp"

	"gitlab.com/gitlab-org/gitaly/v15/internal/command"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
)

// ErrAlreadyLocked indicates a reference cannot be locked because another
// process has already locked it.
type ErrAlreadyLocked struct {
	Ref string
}

func (e *ErrAlreadyLocked) Error() string {
	return fmt.Sprintf("reference is already locked: %q", e.Ref)
}

// ErrInvalidReferenceFormat indicates a reference name was invalid.
type ErrInvalidReferenceFormat struct {
	// ReferenceName is the invalid reference name.
	ReferenceName string
}

func (e ErrInvalidReferenceFormat) Error() string {
	return fmt.Sprintf("invalid reference format: %q", e.ReferenceName)
}

// state represents a possible state the updater can be in.
type state string

const (
	// stateIdle means the updater is ready for a new transaction to start.
	stateIdle state = "idle"
	// stateStarted means the updater has an open transaction and accepts
	// new reference changes.
	stateStarted state = "started"
	// statePrepared means the updater has prepared a transaction and no longer
	// accepts reference changes until the current transaction is committed and
	// a new one started.
	statePrepared state = "prepared"
	// stateClosed means the updater has been closed and is no longer usable.
	stateClosed state = "closed"
)

// invalidStateTransitionError is returned when the updater is used incorrectly.
type invalidStateTransitionError struct {
	// expected is the state the updater was expected to be in.
	expected state
	// actual is the state the updater was actually in.
	actual state
}

// Error returns the formatted error string.
func (err invalidStateTransitionError) Error() string {
	return fmt.Sprintf("expected state %q but it was %q", err.expected, err.actual)
}

// Updater wraps a `git update-ref --stdin` process, presenting an interface
// that allows references to be easily updated in bulk. It is not suitable for
// concurrent use.
//
// Correct usage of the Updater is as follows:
//  1. Transaction must be started before anything else.
//  2. Transaction can't be started if there is an active transaction.
//  3. Updates can be staged only when there is an unprepared transaction.
//  4. Prepare can be called only with an unprepared transaction.
//  5. Commit can be called only with an active transaction. The transaction
//     can be committed unprepared or prepared.
//  7. Close can be called at any time. The active transaction is aborted.
//  8. Any sort of error causes the updater to close.
type Updater struct {
	repo       git.RepositoryExecutor
	cmd        *command.Command
	stdout     *bufio.Reader
	stderr     *bytes.Buffer
	objectHash git.ObjectHash

	// state tracks the current state of the updater to ensure correct calling semantics.
	state state
}

// UpdaterOpt is a type representing options for the Updater.
type UpdaterOpt func(*updaterConfig)

type updaterConfig struct {
	disableTransactions bool
}

// WithDisabledTransactions disables hooks such that no reference-transactions
// are used for the updater.
func WithDisabledTransactions() UpdaterOpt {
	return func(cfg *updaterConfig) {
		cfg.disableTransactions = true
	}
}

// New returns a new bulk updater, wrapping a `git update-ref` process. Call the
// various methods to enqueue updates, then call Commit() to attempt to apply all
// the updates at once.
//
// It is important that ctx gets canceled somewhere. If it doesn't, the process
// spawned by New() may never terminate.
func New(ctx context.Context, repo git.RepositoryExecutor, opts ...UpdaterOpt) (*Updater, error) {
	var cfg updaterConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	txOption := git.WithRefTxHook(repo)
	if cfg.disableTransactions {
		txOption = git.WithDisabledHooks()
	}

	var stderr bytes.Buffer
	cmd, err := repo.Exec(ctx,
		git.SubCmd{
			Name:  "update-ref",
			Flags: []git.Option{git.Flag{Name: "-z"}, git.Flag{Name: "--stdin"}},
		},
		txOption,
		git.WithSetupStdin(),
		git.WithStderr(&stderr),
	)
	if err != nil {
		return nil, err
	}

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return nil, fmt.Errorf("detecting object hash: %w", err)
	}

	return &Updater{
		repo:       repo,
		cmd:        cmd,
		stderr:     &stderr,
		stdout:     bufio.NewReader(cmd),
		objectHash: objectHash,
		state:      stateIdle,
	}, nil
}

// expectState returns an error and closes the updater if it is not in the expected state.
func (u *Updater) expectState(expected state) error {
	if err := u.checkState(expected); err != nil {
		_ = u.Close()
		return err
	}

	return nil
}

// checkState returns an error if the updater is not in the expected state.
func (u *Updater) checkState(expected state) error {
	if u.state != expected {
		return invalidStateTransitionError{expected: expected, actual: u.state}
	}

	return nil
}

// Start begins a new reference transaction. The reference changes are not perfromed until Commit
// is explicitly called.
func (u *Updater) Start() error {
	if err := u.expectState(stateIdle); err != nil {
		return err
	}

	u.state = stateStarted

	return u.setState("start")
}

// Update commands the reference to be updated to point at the object ID specified in newOID. If
// newOID is the zero OID, then the branch will be deleted. If oldOID is a non-empty string, then
// the reference will only be updated if its current value matches the old value. If the old value
// is the zero OID, then the branch must not exist.
//
// A reference transaction must be started before calling Update.
func (u *Updater) Update(reference git.ReferenceName, newOID, oldOID git.ObjectID) error {
	if err := u.expectState(stateStarted); err != nil {
		return err
	}

	return u.write("update %s\x00%s\x00%s\x00", reference.String(), newOID, oldOID)
}

// Create commands the reference to be created with the given object ID. The ref must not exist.
//
// A reference transaction must be started before calling Create.
func (u *Updater) Create(reference git.ReferenceName, oid git.ObjectID) error {
	return u.Update(reference, oid, u.objectHash.ZeroOID)
}

// Delete commands the reference to be removed from the repository. This command will ignore any old
// state of the reference and just force-remove it.
//
// A reference transaction must be started before calling Delete.
func (u *Updater) Delete(reference git.ReferenceName) error {
	return u.Update(reference, u.objectHash.ZeroOID, "")
}

var (
	refLockedRegex        = regexp.MustCompile("cannot lock ref '(.+?)'")
	refInvalidFormatRegex = regexp.MustCompile(`invalid ref format: (.*)\\n"`)
)

// Prepare prepares the reference transaction by locking all references and determining their
// current values. The updates are not yet committed and will be rolled back in case there is no
// call to `Commit()`. This call is optional.
func (u *Updater) Prepare() error {
	if err := u.expectState(stateStarted); err != nil {
		return err
	}

	u.state = statePrepared

	if err := u.setState("prepare"); err != nil {
		matches := refLockedRegex.FindSubmatch([]byte(err.Error()))
		if len(matches) > 1 {
			return &ErrAlreadyLocked{Ref: string(matches[1])}
		}

		matches = refInvalidFormatRegex.FindSubmatch([]byte(err.Error()))
		if len(matches) > 1 {
			return ErrInvalidReferenceFormat{ReferenceName: string(matches[1])}
		}

		return err
	}

	return nil
}

// Commit applies the commands specified in other calls to the Updater. Commit finishes the
// reference transaction and another one must be started before further changes can be staged.
func (u *Updater) Commit() error {
	// Commit can be called without preparing the transactions.
	if err := u.checkState(statePrepared); err != nil {
		if err := u.expectState(stateStarted); err != nil {
			return err
		}
	}

	u.state = stateIdle

	if err := u.setState("commit"); err != nil {
		return err
	}

	return nil
}

// Close closes the updater and aborts a possible open transaction. No changes will be written
// to disk, all lockfiles will be cleaned up and the process will exit.
func (u *Updater) Close() error {
	u.state = stateClosed

	if err := u.cmd.Wait(); err != nil {
		return fmt.Errorf("closing updater: %w", err)
	}
	return nil
}

func (u *Updater) write(format string, args ...interface{}) error {
	if _, err := fmt.Fprintf(u.cmd, format, args...); err != nil {
		// We need to explicitly cancel the command here and wait for it to terminate such
		// that we can retrieve the command's stderr in a race-free manner.
		_ = u.Close()
		return fmt.Errorf("%w: %q", err, u.stderr)
	}

	return nil
}

func (u *Updater) setState(state string) error {
	if err := u.write("%s\x00", state); err != nil {
		return fmt.Errorf("updating state to %q: %w", state, err)
	}

	// For each state-changing command, git-update-ref(1) will report successful execution via
	// "<command>: ok" lines printed to its stdout. Ideally, we should thus verify here whether
	// the command was successfully executed by checking for exactly this line, otherwise we
	// cannot be sure whether the command has correctly been processed by Git or if an error was
	// raised.
	line, err := u.stdout.ReadString('\n')
	if err != nil {
		// We need to explicitly cancel the command here and wait for it to
		// terminate such that we can retrieve the command's stderr in a race-free
		// manner.
		_ = u.Close()
		return fmt.Errorf("state update to %q failed: %w, stderr: %q", state, err, u.stderr)
	}

	if line != fmt.Sprintf("%s: ok\n", state) {
		_ = u.Close()
		return fmt.Errorf("state update to %q not successful: expected ok, got %q", state, line)
	}

	return nil
}
