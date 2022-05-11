package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v15/auth"
	"gitlab.com/gitlab-org/gitaly/v15/client"
	"gitlab.com/gitlab-org/gitaly/v15/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/labkit/tracing"
	"google.golang.org/grpc"
)

type packFn func(context.Context, *grpc.ClientConn, *client.SidechannelRegistry, string) (int32, error)

type gitalySSHCommand struct {
	// The git packer that shall be executed. One of receivePack,
	// uploadPack or uploadArchive
	packer packFn
	// Working directory to execute the packer in
	workingDir string
	// Address of the server we want to post the request to
	address string
	// Marshalled gRPC payload to pass to the remote server
	payload string
	// Comma separated list of feature flags that shall be enabled on the
	// remote server
	featureFlags string
}

// GITALY_ADDRESS="tcp://1.2.3.4:9999" or "unix:/var/run/gitaly.sock"
// GITALY_TOKEN="foobar1234"
// GITALY_PAYLOAD="{repo...}"
// GITALY_WD="/path/to/working-directory"
// GITALY_FEATUREFLAGS="upload_pack_filter:false,hooks_rpc:true"
// GITALY_USE_SIDECHANNEL=1 if desired
// gitaly-ssh upload-pack <git-garbage-x2>
func main() {
	// < 4 since git throws on 2x garbage here
	if n := len(os.Args); n < 4 {
		// TODO: Errors needs to be sent back some other way... pipes?
		log.Fatalf("invalid number of arguments, expected at least 1, got %d", n-1)
	}

	command := os.Args[1]
	var packer packFn
	switch command {
	case "upload-pack":
		if useSidechannel() {
			packer = uploadPackWithSidechannel
		} else {
			packer = uploadPack
		}
	case "receive-pack":
		packer = receivePack
	case "upload-archive":
		packer = uploadArchive
	default:
		log.Fatalf("invalid pack command: %q", command)
	}

	cmd := gitalySSHCommand{
		packer:       packer,
		workingDir:   os.Getenv("GITALY_WD"),
		address:      os.Getenv("GITALY_ADDRESS"),
		payload:      os.Getenv("GITALY_PAYLOAD"),
		featureFlags: os.Getenv("GITALY_FEATUREFLAGS"),
	}

	code, err := cmd.run()
	if err != nil {
		log.Printf("%s: %v", command, err)
	}

	os.Exit(code)
}

func (cmd gitalySSHCommand) run() (int, error) {
	// Configure distributed tracing
	closer := tracing.Initialize(tracing.WithServiceName("gitaly-ssh"))
	defer closer.Close()

	ctx, finished := tracing.ExtractFromEnv(context.Background())
	defer finished()

	if cmd.featureFlags != "" {
		for _, flagPair := range strings.Split(cmd.featureFlags, ",") {
			flagPairSplit := strings.SplitN(flagPair, ":", 2)
			if len(flagPairSplit) != 2 {
				continue
			}

			enabled, err := strconv.ParseBool(flagPairSplit[1])
			if err != nil {
				continue
			}

			ctx = featureflag.OutgoingCtxWithFeatureFlag(ctx, featureflag.FeatureFlag{Name: flagPairSplit[0]}, enabled)
		}
	}

	if cmd.workingDir != "" {
		if err := os.Chdir(cmd.workingDir); err != nil {
			return 1, fmt.Errorf("unable to chdir to %v", cmd.workingDir)
		}
	}

	registry := client.NewSidechannelRegistry(logrus.NewEntry(logrus.StandardLogger()))
	conn, err := getConnection(ctx, cmd.address, registry)
	if err != nil {
		return 1, err
	}
	defer conn.Close()

	code, err := cmd.packer(ctx, conn, registry, cmd.payload)
	if err != nil {
		return 1, err
	}

	return int(code), nil
}

func getConnection(ctx context.Context, url string, registry *client.SidechannelRegistry) (*grpc.ClientConn, error) {
	if url == "" {
		return nil, fmt.Errorf("gitaly address can not be empty")
	}

	if useSidechannel() {
		return client.DialSidechannel(ctx, url, registry, dialOpts())
	}

	return client.DialContext(ctx, url, dialOpts())
}

func dialOpts() []grpc.DialOption {
	connOpts := client.DefaultDialOpts
	if token := os.Getenv("GITALY_TOKEN"); token != "" {
		connOpts = append(connOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token)))
	}

	return connOpts
}

func useSidechannel() bool { return os.Getenv("GITALY_USE_SIDECHANNEL") == "1" }
