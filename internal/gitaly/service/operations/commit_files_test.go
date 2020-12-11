	"encoding/base64"
	"io/ioutil"
	"os"
	"path/filepath"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"google.golang.org/grpc/status"
func testImplementations(t *testing.T, test func(t *testing.T, ctx context.Context)) {
	goCtx, cancel := testhelper.Context()
	defer cancel()

	rubyCtx := featureflag.OutgoingCtxWithDisabledFeatureFlags(goCtx, featureflag.GoUserCommitFiles)

	for _, tc := range []struct {
		desc    string
		context context.Context
	}{
		{desc: "go", context: goCtx},
		{desc: "ruby", context: rubyCtx},
	} {
		t.Run(tc.desc, func(t *testing.T) { test(t, tc.context) })
	}
}

func TestUserCommitFiles(t *testing.T) {
	testImplementations(t, testUserCommitFiles)
}

func testUserCommitFiles(t *testing.T, ctx context.Context) {
	const (
		DefaultMode    = "100644"
		ExecutableMode = "100755"
	)

	// Multiple locations in the call path depend on the global configuration.
	// This creates a clean directory in the test storage. We then recreate the
	// repository there on every test run. This allows us to use deterministic
	// paths in the tests.
	storageRoot, err := ioutil.TempDir(testhelper.GitlabTestStoragePath(), "")
	require.NoError(t, err)
	defer os.RemoveAll(storageRoot)

	const storageName = "default"
	relativePath, err := filepath.Rel(testhelper.GitlabTestStoragePath(), filepath.Join(storageRoot, "test-repository"))
	require.NoError(t, err)

	repoPath := filepath.Join(testhelper.GitlabTestStoragePath(), relativePath)

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	for key, values := range testhelper.GitalyServersMetadata(t, serverSocketPath) {
		for _, value := range values {
			ctx = metadata.AppendToOutgoingContext(ctx, key, value)
		}
	}

	type step struct {
		actions       []*gitalypb.UserCommitFilesRequest
		changeHeader  func(*gitalypb.UserCommitFilesRequest)
		error         error
		indexError    string
		repoCreated   bool
		branchCreated bool
		treeEntries   []testhelper.TreeEntry
	}

	for _, tc := range []struct {
		desc  string
		steps []step
	}{
		{
			desc: "create directory",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "directory-1/.gitkeep"},
					},
				},
			},
		},
		{
			desc: "create directory ignores mode and content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action:          gitalypb.UserCommitFilesActionHeader_CREATE_DIR,
									FilePath:        []byte("directory-1"),
									ExecuteFilemode: true,
									Base64Content:   true,
								},
							},
						}),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "directory-1/.gitkeep"},
					},
				},
			},
		},
		{
			desc: "create directory created duplicate",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
						createDirHeaderRequest("directory-1"),
					},
					indexError: "A directory with this name already exists",
				},
			},
		},
		{
			desc: "create directory with traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("../directory-1"),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "create directory existing duplicate",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "directory-1/.gitkeep"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
					},
					indexError: "A directory with this name already exists",
				},
			},
		},
		{
			desc: "create directory with files name",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("file-1"),
					},
					indexError: "A file with this name already exists",
				},
			},
		},
		{
			desc: "create file with directory traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("../file-1"),
						actionContentRequest("content-1"),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "create file without content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1"},
					},
				},
			},
		},
		{
			desc: "create file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						actionContentRequest(" content-2"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1 content-2"},
					},
				},
			},
		},
		{
			desc: "create file with unclean path",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("/file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "create file with base64 content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createBase64FileHeaderRequest("file-1"),
						actionContentRequest(base64.StdEncoding.EncodeToString([]byte("content-1"))),
						actionContentRequest(base64.StdEncoding.EncodeToString([]byte(" content-2"))),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1 content-2"},
					},
				},
			},
		},
		{
			desc: "create duplicate file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						createFileHeaderRequest("file-1"),
					},
					indexError: "A file with this name already exists",
				},
			},
		},
		{
			desc: "create file overwrites directory",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("file-1"),
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "update created file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						updateFileHeaderRequest("file-1"),
						actionContentRequest("content-2"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update base64 content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						updateBase64FileHeaderRequest("file-1"),
						actionContentRequest(base64.StdEncoding.EncodeToString([]byte("content-2"))),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update ignores mode",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action:          gitalypb.UserCommitFilesActionHeader_UPDATE,
									FilePath:        []byte("file-1"),
									ExecuteFilemode: true,
								},
							},
						}),
						actionContentRequest("content-2"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						updateFileHeaderRequest("file-1"),
						actionContentRequest("content-2"),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update non-existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						updateFileHeaderRequest("non-existing"),
						actionContentRequest("content"),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "move file with traversal in source",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("../original-file", "moved-file", true),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "move file with traversal in destination",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("original-file", "../moved-file", true),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "move created file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("content-1"),
						moveFileHeaderRequest("original-file", "moved-file", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "move ignores mode",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("content-1"),
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action:          gitalypb.UserCommitFilesActionHeader_MOVE,
									FilePath:        []byte("moved-file"),
									PreviousPath:    []byte("original-file"),
									ExecuteFilemode: true,
									InferContent:    true,
								},
							},
						}),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "moving directory fails",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory"),
						moveFileHeaderRequest("directory", "moved-directory", true),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "move file inferring content",
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
						moveFileHeaderRequest("original-file", "moved-file", true),
						actionContentRequest("ignored-content"),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "original-content"},
					},
				},
			},
		},
		{
			desc: "move file with non-existing source",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("non-existing", "destination-file", true),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "move file with already existing destination file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("source-file"),
						createFileHeaderRequest("already-existing"),
						moveFileHeaderRequest("source-file", "already-existing", true),
					},
					indexError: "A file with this name already exists",
				},
			},
		},
		{
			// seems like a bug in the original implementation to allow overwriting a
			// directory
			desc: "move file with already existing destination directory",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("source-file"),
						actionContentRequest("source-content"),
						createDirHeaderRequest("already-existing"),
						moveFileHeaderRequest("source-file", "already-existing", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "already-existing", Content: "source-content"},
					},
				},
			},
		},
		{
			desc: "move file providing content",
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
						actionContentRequest("new-content"),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "new-content"},
					},
				},
			},
		},
		{
			desc: "mark non-existing file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("file-1", true),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "mark executable file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						chmodFileHeaderRequest("file-1", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("file-1", true),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1"},
					},
				},
			},
		},
		{
			desc: "mark file executable with directory traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("../file-1", true),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "mark created file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						chmodFileHeaderRequest("file-1", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "mark existing file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("file-1", true),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "move non-existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("non-existing", "should-not-be-created", true),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "move doesn't overwrite a file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						createFileHeaderRequest("file-2"),
						actionContentRequest("content-2"),
						moveFileHeaderRequest("file-1", "file-2", true),
					},
					indexError: "A file with this name already exists",
				},
			},
		},
		{
			desc: "delete non-existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						deleteFileHeaderRequest("non-existing"),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "delete file with directory traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						deleteFileHeaderRequest("../file-1"),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "delete created file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						deleteFileHeaderRequest("file-1"),
					},
					branchCreated: true,
					repoCreated:   true,
				},
			},
		},
		{
			desc: "delete existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					branchCreated: true,
					repoCreated:   true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						deleteFileHeaderRequest("file-1"),
					},
				},
			},
		},
		{
			desc: "invalid action",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action: -1,
								},
							},
						}),
					},
					error: status.Error(codes.Unknown, "NoMethodError: undefined method `downcase' for -1:Integer"),
				},
			},
		},
		{
			desc: "start repository refers to target repository",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					changeHeader: func(header *gitalypb.UserCommitFilesRequest) {
						setStartRepository(header, &gitalypb.Repository{
							StorageName:  storageName,
							RelativePath: relativePath,
						})
					},
					branchCreated: true,
					repoCreated:   true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			defer os.RemoveAll(repoPath)
			testhelper.MustRunCommand(t, nil, "git", "init", "--bare", repoPath)

			const branch = "master"

			for i, step := range tc.steps {
				stream, err := client.UserCommitFiles(ctx)
				require.NoError(t, err)

				headerRequest := headerRequest(
					testhelper.CreateRepo(t, storageRoot, relativePath),
					testhelper.TestUser,
					branch,
					[]byte("commit message"),
				)
				setAuthorAndEmail(headerRequest, []byte("Author Name"), []byte("author.email@example.com"))

				if step.changeHeader != nil {
					step.changeHeader(headerRequest)
				}

				require.NoError(t, stream.Send(headerRequest))

				for j, action := range step.actions {
					require.NoError(t, stream.Send(action), "step %d, action %d", i+1, j+1)
				}

				resp, err := stream.CloseAndRecv()
				require.Equal(t, step.error, err)
				if step.error != nil {
					continue
				}

				require.Equal(t, step.indexError, resp.IndexError, "step %d", i+1)
				if step.indexError != "" {
					continue
				}

				require.Equal(t, step.branchCreated, resp.BranchUpdate.BranchCreated, "step %d", i+1)
				require.Equal(t, step.repoCreated, resp.BranchUpdate.RepoCreated, "step %d", i+1)
				testhelper.RequireTree(t, repoPath, branch, step.treeEntries)
			}
		})
	}
}

func TestSuccessfulUserCommitFilesRequest(t *testing.T) {
	testImplementations(t, testSuccessfulUserCommitFilesRequest)
}

func testSuccessfulUserCommitFilesRequest(t *testing.T, ctx context.Context) {
			headCommit, err := log.GetCommit(ctx, tc.repo, tc.branchName)
func TestSuccessfulUserCommitFilesRequestMove(t *testing.T) {
	testImplementations(t, testSuccessfulUserCommitFilesRequestMove)
func testSuccessfulUserCommitFilesRequestMove(t *testing.T, ctx context.Context) {
	testImplementations(t, testSuccessfulUserCommitFilesRequestForceCommit)
}

func testSuccessfulUserCommitFilesRequestForceCommit(t *testing.T, ctx context.Context) {
	startBranchCommit, err := log.GetCommit(ctx, testRepo, string(startBranchName))
	targetBranchCommit, err := log.GetCommit(ctx, testRepo, targetBranchName)
	newTargetBranchCommit, err := log.GetCommit(ctx, testRepo, targetBranchName)
	testImplementations(t, testSuccessfulUserCommitFilesRequestStartSha)
}

func testSuccessfulUserCommitFilesRequestStartSha(t *testing.T, ctx context.Context) {
	startCommit, err := log.GetCommit(ctx, testRepo, "master")
	newTargetBranchCommit, err := log.GetCommit(ctx, testRepo, targetBranchName)
	testImplementations(t, testSuccessfulUserCommitFilesRemoteRepositoryRequest(func(header *gitalypb.UserCommitFilesRequest) {
		setStartSha(header, "1e292f8fedd741b75372e19097c76d327140c312")
	}))
}
func TestSuccessfulUserCommitFilesRequestStartBranchRemoteRepository(t *testing.T) {
	testImplementations(t, testSuccessfulUserCommitFilesRemoteRepositoryRequest(func(header *gitalypb.UserCommitFilesRequest) {
		setStartBranchName(header, []byte("master"))
	}))
}
func testSuccessfulUserCommitFilesRemoteRepositoryRequest(setHeader func(header *gitalypb.UserCommitFilesRequest)) func(*testing.T, context.Context) {
	// Regular table driven test did not work here as there is some state shared in the helpers between the subtests.
	// Running them in different top level tests works, so we use a parameterized function instead to share the code.
	return func(t *testing.T, ctx context.Context) {
		serverSocketPath, stop := runOperationServiceServer(t)
		defer stop()
		client, conn := newOperationClient(t, serverSocketPath)
		defer conn.Close()
		testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
		defer cleanupFn()
		newRepo, _, newRepoCleanupFn := testhelper.InitBareRepo(t)
		defer newRepoCleanupFn()
		for key, values := range testhelper.GitalyServersMetadata(t, serverSocketPath) {
			for _, value := range values {
				ctx = metadata.AppendToOutgoingContext(ctx, key, value)
			}
		}
		targetBranchName := "new"
		startCommit, err := log.GetCommit(ctx, testRepo, "master")
		require.NoError(t, err)
		headerRequest := headerRequest(newRepo, testhelper.TestUser, targetBranchName, commitFilesMessage)
		setHeader(headerRequest)
		setStartRepository(headerRequest, testRepo)
		stream, err := client.UserCommitFiles(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(headerRequest))
		require.NoError(t, stream.Send(createFileHeaderRequest("TEST.md")))
		require.NoError(t, stream.Send(actionContentRequest("Test")))
		resp, err := stream.CloseAndRecv()
		require.NoError(t, err)

		update := resp.GetBranchUpdate()
		newTargetBranchCommit, err := log.GetCommit(ctx, newRepo, targetBranchName)
		require.NoError(t, err)

		require.Equal(t, newTargetBranchCommit.Id, update.CommitId)
		require.Equal(t, newTargetBranchCommit.ParentIds, []string{startCommit.Id})
	}
	testImplementations(t, testSuccessfulUserCommitFilesRequestWithSpecialCharactersInSignature)
}

func testSuccessfulUserCommitFilesRequestWithSpecialCharactersInSignature(t *testing.T, ctx context.Context) {
			newCommit, err := log.GetCommit(ctx, testRepo, targetBranchName)
	testImplementations(t, testFailedUserCommitFilesRequestDueToHooks)
}

func testFailedUserCommitFilesRequestDueToHooks(t *testing.T, ctx context.Context) {

	testImplementations(t, testFailedUserCommitFilesRequestDueToIndexError)
}

func testFailedUserCommitFilesRequestDueToIndexError(t *testing.T, ctx context.Context) {
	testImplementations(t, testFailedUserCommitFilesRequest)
}

func testFailedUserCommitFilesRequest(t *testing.T, ctx context.Context) {
func createDirHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:   gitalypb.UserCommitFilesActionHeader_CREATE_DIR,
				FilePath: []byte(filePath),
			},
		},
	})
}

func createBase64FileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:        gitalypb.UserCommitFilesActionHeader_CREATE,
				Base64Content: true,
				FilePath:      []byte(filePath),
			},
		},
	})
}

func updateFileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:   gitalypb.UserCommitFilesActionHeader_UPDATE,
				FilePath: []byte(filePath),
			},
		},
	})
}

func updateBase64FileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:        gitalypb.UserCommitFilesActionHeader_UPDATE,
				FilePath:      []byte(filePath),
				Base64Content: true,
			},
		},
	})
}

func deleteFileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:   gitalypb.UserCommitFilesActionHeader_DELETE,
				FilePath: []byte(filePath),
			},
		},
	})
}
