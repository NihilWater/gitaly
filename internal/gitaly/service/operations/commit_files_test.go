	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	targetRelativePath, err := filepath.Rel(testhelper.GitlabTestStoragePath(), filepath.Join(storageRoot, "target-repository"))
	startRepo, _, cleanStartRepo := testhelper.InitBareRepo(t)
	defer cleanStartRepo()

	repoPath := filepath.Join(testhelper.GitlabTestStoragePath(), targetRelativePath)
	ctxWithServerMetadata := ctx
			ctxWithServerMetadata = metadata.AppendToOutgoingContext(ctxWithServerMetadata, key, value)
		actions         []*gitalypb.UserCommitFilesRequest
		startRepository *gitalypb.Repository
		error           error
		indexError      string
		repoCreated     bool
		branchCreated   bool
		treeEntries     []testhelper.TreeEntry
					startRepository: &gitalypb.Repository{
						StorageName:  storageName,
						RelativePath: targetRelativePath,
		{
			desc: "start repository refers to an empty repository",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					startRepository: startRepo,
					branchCreated:   true,
					repoCreated:     true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
					testhelper.CreateRepo(t, storageRoot, targetRelativePath),
				ctx := ctx
				if step.startRepository != nil {
					ctx = ctxWithServerMetadata
					setStartRepository(headerRequest, step.startRepository)
				stream, err := client.UserCommitFiles(ctx)
				require.NoError(t, err)
	locator := config.NewLocator(config.Config)

			headCommit, err := log.GetCommit(ctx, locator, tc.repo, tc.branchName)
	locator := config.NewLocator(config.Config)

	startBranchCommit, err := log.GetCommit(ctx, locator, testRepo, string(startBranchName))
	targetBranchCommit, err := log.GetCommit(ctx, locator, testRepo, targetBranchName)
	newTargetBranchCommit, err := log.GetCommit(ctx, locator, testRepo, targetBranchName)
	locator := config.NewLocator(config.Config)

	startCommit, err := log.GetCommit(ctx, locator, testRepo, "master")
	newTargetBranchCommit, err := log.GetCommit(ctx, locator, testRepo, targetBranchName)
		locator := config.NewLocator(config.Config)

		startCommit, err := log.GetCommit(ctx, locator, testRepo, "master")
		newTargetBranchCommit, err := log.GetCommit(ctx, locator, newRepo, targetBranchName)
	locator := config.NewLocator(config.Config)

			newCommit, err := log.GetCommit(ctx, locator, testRepo, targetBranchName)