//go:build !gitaly_test_sha256

package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v15/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func registerPraefectInfoServer(impl gitalypb.PraefectInfoServiceServer) svcRegistrar {
	return func(srv *grpc.Server) {
		gitalypb.RegisterPraefectInfoServiceServer(srv, impl)
	}
}

func TestDatalossSubcommand(t *testing.T) {
	t.Parallel()
	cfg := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "virtual-storage-1",
				Nodes: []*config.Node{
					{Storage: "gitaly-1"},
					{Storage: "gitaly-2"},
					{Storage: "gitaly-3"},
				},
			},
			{
				Name: "virtual-storage-2",
				Nodes: []*config.Node{
					{Storage: "gitaly-4"},
				},
			},
		},
	}

	tx := testdb.New(t).Begin(t)
	defer tx.Rollback(t)
	ctx := testhelper.Context(t)

	testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{"praefect-0": {
		"virtual-storage-1": {"gitaly-1", "gitaly-3"},
	}})
	gs := datastore.NewPostgresRepositoryStore(tx, cfg.StorageNames())

	for _, q := range []string{
		`
				INSERT INTO repositories (repository_id, virtual_storage, relative_path, "primary")
				VALUES
					(1, 'virtual-storage-1', 'repository-1', 'gitaly-1'),
					(2, 'virtual-storage-1', 'repository-2', 'gitaly-3')
				`,
		`
				INSERT INTO repository_assignments (repository_id, virtual_storage, relative_path, storage)
				VALUES
					(1, 'virtual-storage-1', 'repository-1', 'gitaly-1'),
					(1, 'virtual-storage-1', 'repository-1', 'gitaly-2'),
					(2, 'virtual-storage-1', 'repository-2', 'gitaly-1'),
					(2, 'virtual-storage-1', 'repository-2', 'gitaly-3')
				`,
	} {
		_, err := tx.ExecContext(ctx, q)
		require.NoError(t, err)
	}

	require.NoError(t, gs.SetGeneration(ctx, 1, "gitaly-1", "repository-1", 1))
	require.NoError(t, gs.SetGeneration(ctx, 1, "gitaly-2", "repository-1", 0))
	require.NoError(t, gs.SetGeneration(ctx, 1, "gitaly-3", "repository-1", 0))

	require.NoError(t, gs.SetGeneration(ctx, 2, "gitaly-2", "repository-2", 1))
	require.NoError(t, gs.SetGeneration(ctx, 2, "gitaly-3", "repository-2", 0))

	ln, clean := listenAndServe(t, []svcRegistrar{
		registerPraefectInfoServer(info.NewServer(cfg, gs, nil, nil, nil)),
	})
	defer clean()
	for _, tc := range []struct {
		desc            string
		args            []string
		virtualStorages []*config.VirtualStorage
		output          string
		error           error
	}{
		{
			desc:  "positional arguments",
			args:  []string{"-virtual-storage=virtual-storage-1", "positional-arg"},
			error: unexpectedPositionalArgsError{Command: "dataloss"},
		},
		{
			desc: "data loss with unavailable repositories",
			args: []string{"-virtual-storage=virtual-storage-1"},
			output: `Virtual storage: virtual-storage-1
  Repositories:
    repository-2 (unavailable):
      Primary: gitaly-3
      In-Sync Storages:
        gitaly-2, unhealthy
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
`,
		},
		{
			desc: "data loss with partially unavailable repositories",
			args: []string{"-virtual-storage=virtual-storage-1", "-partially-unavailable"},
			output: `Virtual storage: virtual-storage-1
  Repositories:
    repository-1:
      Primary: gitaly-1
      In-Sync Storages:
        gitaly-1, assigned host
      Outdated Storages:
        gitaly-2 is behind by 1 change or less, assigned host, unhealthy
        gitaly-3 is behind by 1 change or less
    repository-2 (unavailable):
      Primary: gitaly-3
      In-Sync Storages:
        gitaly-2, unhealthy
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
`,
		},
		{
			desc:            "multiple virtual storages with unavailable repositories",
			virtualStorages: []*config.VirtualStorage{{Name: "virtual-storage-2"}, {Name: "virtual-storage-1"}},
			output: `Virtual storage: virtual-storage-1
  Repositories:
    repository-2 (unavailable):
      Primary: gitaly-3
      In-Sync Storages:
        gitaly-2, unhealthy
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
Virtual storage: virtual-storage-2
  All repositories are available!
`,
		},
		{
			desc:            "multiple virtual storages with partially unavailable repositories",
			args:            []string{"-partially-unavailable"},
			virtualStorages: []*config.VirtualStorage{{Name: "virtual-storage-2"}, {Name: "virtual-storage-1"}},
			output: `Virtual storage: virtual-storage-1
  Repositories:
    repository-1:
      Primary: gitaly-1
      In-Sync Storages:
        gitaly-1, assigned host
      Outdated Storages:
        gitaly-2 is behind by 1 change or less, assigned host, unhealthy
        gitaly-3 is behind by 1 change or less
    repository-2 (unavailable):
      Primary: gitaly-3
      In-Sync Storages:
        gitaly-2, unhealthy
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
Virtual storage: virtual-storage-2
  All repositories are fully available on all assigned storages!
`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cmd := newDatalossSubcommand()
			output := &bytes.Buffer{}
			cmd.output = output

			fs := cmd.FlagSet()
			require.NoError(t, fs.Parse(tc.args))
			err := cmd.Exec(fs, config.Config{
				VirtualStorages: tc.virtualStorages,
				SocketPath:      ln.Addr().String(),
			})
			require.Equal(t, tc.error, err, err)
			require.Equal(t, tc.output, output.String())
		})
	}
}
