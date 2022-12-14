//go:build static && system_libgit2

package commit

import (
	git "github.com/libgit2/git2go/v34"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git2go"
)

func applyDeleteFile(action git2go.DeleteFile, index *git.Index) error {
	if err := validateFileExists(index, action.Path); err != nil {
		return err
	}

	return index.RemoveByPath(action.Path)
}
