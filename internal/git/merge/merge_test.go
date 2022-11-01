package merge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseResult(t *testing.T) {
	testCases := []struct {
		desc        string
		output      string
		oid         string
		expectedErr *MergeTreeError
	}{
		{
			desc:   "success",
			output: "4f18fcb9bc62a3a6a4f81236fd57eeb95bfb06b4",
			oid:    "4f18fcb9bc62a3a6a4f81236fd57eeb95bfb06b4",
		},
		{
			desc: "single file conflict",
			output: `4f18fcb9bc62a3a6a4f81236fd57eeb95bfb06b4
file

Auto-merging file
CONFLICT (content): Merge conflict in file
`,
			oid: "4f18fcb9bc62a3a6a4f81236fd57eeb95bfb06b4",
			expectedErr: &MergeTreeError{
				ConflictingFiles: []string{
					"file",
				},
				InfoMessage: "Auto-merging file\nCONFLICT (content): Merge conflict in file",
			},
		},
		{
			desc: "multiple files conflict",
			output: `4f18fcb9bc62a3a6a4f81236fd57eeb95bfb06b4
file1
file2

Auto-merging file
CONFLICT (content): Merge conflict in file1
`,
			oid: "4f18fcb9bc62a3a6a4f81236fd57eeb95bfb06b4",
			expectedErr: &MergeTreeError{
				ConflictingFiles: []string{
					"file1",
					"file2",
				},
				InfoMessage: "Auto-merging file\nCONFLICT (content): Merge conflict in file1",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			oid, err := parseResult(tc.output)
			if tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.oid, string(oid))
		})
	}
}
