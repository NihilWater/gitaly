package praefect

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	gconfig "gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestInfoService_RepositoryReplicas(t *testing.T) {
	cfg := gconfig.Config

	tempDir, cleanupTempDir := testhelper.TempDir(t)
	defer cleanupTempDir()

	cfg.Storages = []gconfig.Storage{{Name: "gitaly-1"}, {Name: "gitaly-2"}, {Name: "gitaly-3"}}
	for i := range cfg.Storages {
		storagePath := filepath.Join(tempDir, cfg.Storages[i].Name)
		require.NoError(t, os.MkdirAll(storagePath, 0755))
		cfg.Storages[i].Path = storagePath
	}

	// TODO: this should be removed once we get rid of git.NewCommand function and replace it's usage with git.CommandFactory
	defer func(old []gconfig.Storage) { gconfig.Config.Storages = old }(gconfig.Config.Storages)
	gconfig.Config.Storages = cfg.Storages

	gitalyAddr, cleanupGitaly := testserver.RunGitalyServer(t, cfg, nil)
	defer cleanupGitaly()

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{
						Storage: "gitaly-1",
						Address: gitalyAddr,
						Token:   cfg.Auth.Token,
					},
					{
						Storage: "gitaly-2",
						Address: gitalyAddr,
						Token:   cfg.Auth.Token,
					},
					{
						Storage: "gitaly-3",
						Address: gitalyAddr,
						Token:   cfg.Auth.Token,
					},
				},
			},
		},
		Failover: config.Failover{Enabled: true},
	}

	testRepo := testhelper.NewTestRepoTo(t, cfg.Storages[0].Path, "repo-1")
	testhelper.NewTestRepoTo(t, cfg.Storages[1].Path, "repo-1")
	testhelper.NewTestRepoTo(t, cfg.Storages[2].Path, "repo-1")

	// create a commit in the second replica so we can check that its checksum is different than the primary
	testhelper.CreateCommit(t, filepath.Join(cfg.Storages[1].Path, "repo-1"), "master", nil)

	cc, _, cleanup := runPraefectServer(t, conf, buildOptions{})
	defer cleanup()

	client := gitalypb.NewPraefectInfoServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	// CalculateChecksum through praefect will get the checksum of the primary
	repoClient := gitalypb.NewRepositoryServiceClient(cc)
	checksum, err := repoClient.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{
		Repository: &gitalypb.Repository{
			StorageName:  conf.VirtualStorages[0].Name,
			RelativePath: testRepo.GetRelativePath(),
		},
	})
	require.NoError(t, err)

	resp, err := client.RepositoryReplicas(ctx, &gitalypb.RepositoryReplicasRequest{
		Repository: &gitalypb.Repository{
			StorageName:  conf.VirtualStorages[0].Name,
			RelativePath: testRepo.GetRelativePath(),
		},
	})

	require.NoError(t, err)

	require.Equal(t, checksum.Checksum, resp.Primary.Checksum)
	var checked []string
	for _, secondary := range resp.GetReplicas() {
		switch storage := secondary.GetRepository().GetStorageName(); storage {
		case conf.VirtualStorages[0].Nodes[1].Storage:
			require.NotEqual(t, checksum.Checksum, secondary.Checksum, "should not be equal since we added a commit")
			checked = append(checked, storage)
		case conf.VirtualStorages[0].Nodes[2].Storage:
			require.Equal(t, checksum.Checksum, secondary.Checksum)
			checked = append(checked, storage)
		default:
			require.FailNow(t, "unexpected storage: %q", storage)
		}
	}
	require.ElementsMatch(t, []string{conf.VirtualStorages[0].Nodes[1].Storage, conf.VirtualStorages[0].Nodes[2].Storage}, checked)
}
