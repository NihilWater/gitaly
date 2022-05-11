package housekeeping

import (
	"regexp"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/praefectutil"
)

// railsPoolDirRegexp is used to validate object pool directory structure and name as generated by Rails.
var railsPoolDirRegexp = regexp.MustCompile(`^@pools/([0-9a-f]{2})/([0-9a-f]{2})/([0-9a-f]{64})\.git$`)

// IsRailsPoolPath returns whether the relative path indicates this is a pool path generated by Rails.
func IsRailsPoolPath(relativePath string) bool {
	matches := railsPoolDirRegexp.FindStringSubmatch(relativePath)
	if matches == nil || !strings.HasPrefix(matches[3], matches[1]+matches[2]) {
		return false
	}

	return true
}

// IsPoolPath returns whether the relative path indicates the repository is an object
// pool.
func IsPoolPath(relativePath string) bool {
	return IsRailsPoolPath(relativePath) || praefectutil.IsPoolPath(relativePath)
}
