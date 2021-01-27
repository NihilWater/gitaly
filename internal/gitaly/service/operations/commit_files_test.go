	"time"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"gitlab.com/gitlab-org/gitaly/internal/git"
		startBranch     string
		{
			desc: "create file with double slash",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("invalid://file/name/here"),
						actionContentRequest("content-1"),
					},
					indexError: "invalid path: 'invalid://file/name/here'",
				},
			},
		},
		{
			desc: "create file normalizes line endings",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1\r\n"),
						actionContentRequest(" content-2\r\n"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1\n content-2\n"},
					},
				},
			},
		},
		{
			desc: "update file normalizes line endings",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						updateFileHeaderRequest("file-1"),
						actionContentRequest("content-2\r\n"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2\n"},
					},
				},
			},
		},
		{
			desc: "move file normalizes line endings",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("original-content"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original-content"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("original-file", "moved-file", false),
						actionContentRequest("new-content\r\n"),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "new-content\n"},
					},
				},
			},
		},
			desc: "empty target repository with start branch set",
					startBranch:   "master",
					branchCreated: true,
					repoCreated:   true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "start repository refers to an empty remote repository",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					startBranch:     "master",
				if step.startBranch != "" {
					setStartBranchName(headerRequest, []byte(step.startBranch))
				}

func TestUserCommitFiles_stableCommitID(t *testing.T) {
	testImplementations(t, testUserCommitFilesStableCommitID)
}

func testUserCommitFilesStableCommitID(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repo, repoPath, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()

	for key, values := range testhelper.GitalyServersMetadata(t, serverSocketPath) {
		for _, value := range values {
			ctx = metadata.AppendToOutgoingContext(ctx, key, value)
		}
	}

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)

	headerRequest := headerRequest(repo, testhelper.TestUser, "master", []byte("commit message"))
	setAuthorAndEmail(headerRequest, []byte("Author Name"), []byte("author.email@example.com"))
	setTimestamp(t, headerRequest, time.Unix(12345, 0))
	require.NoError(t, stream.Send(headerRequest))

	require.NoError(t, stream.Send(createFileHeaderRequest("file.txt")))
	require.NoError(t, stream.Send(actionContentRequest("content")))
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, resp.BranchUpdate.CommitId, "4f0ca1fbf05e04dbd5f68d14677034e0afee58ff")
	require.True(t, resp.BranchUpdate.BranchCreated)
	require.True(t, resp.BranchUpdate.RepoCreated)
	testhelper.RequireTree(t, repoPath, "refs/heads/master", []testhelper.TreeEntry{
		{Mode: "100644", Path: "file.txt", Content: "content"},
	})

	commit, err := log.GetCommit(ctx, config.NewLocator(config.Config), repo, "refs/heads/master")
	require.NoError(t, err)
	require.Equal(t, &gitalypb.GitCommit{
		Id:       "4f0ca1fbf05e04dbd5f68d14677034e0afee58ff",
		TreeId:   "541550ddcf8a29bcd80b0800a142a7d47890cfd6",
		Subject:  []byte("commit message"),
		Body:     []byte("commit message"),
		BodySize: 14,
		Author: &gitalypb.CommitAuthor{
			Name:     []byte("Author Name"),
			Email:    []byte("author.email@example.com"),
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     testhelper.TestUser.Name,
			Email:    testhelper.TestUser.Email,
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
	}, commit)
}

			headCommit, err := log.GetCommit(ctx, locator, tc.repo, git.Revision(tc.branchName))
	startBranchCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(startBranchName))
	targetBranchCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(targetBranchName))
	newTargetBranchCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(targetBranchName))
	newTargetBranchCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(targetBranchName))
		newTargetBranchCommit, err := log.GetCommit(ctx, locator, newRepo, git.Revision(targetBranchName))
			newCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(targetBranchName))
			desc: "invalid object ID: \"foobar\"",
func setTimestamp(t testing.TB, headerRequest *gitalypb.UserCommitFilesRequest, time time.Time) {
	timestamp, err := ptypes.TimestampProto(time)
	require.NoError(t, err)
	getHeader(headerRequest).Timestamp = timestamp
}
