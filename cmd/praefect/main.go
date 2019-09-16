package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/labkit/tracing"
)

var (
	flagConfig  = flag.String("config", "", "Location for the config.toml")
	flagVersion = flag.Bool("version", false, "Print version and exit")
	logger      = log.Default()

	errNoConfigFile = errors.New("the config flag must be passed")
)

func main() {
	flag.Parse()

	// If invoked with -version
	if *flagVersion {
		fmt.Println(praefect.GetVersionString())
		os.Exit(0)
	}

	conf, err := configure()
	if err != nil {
		logger.Fatal(err)
	}

	listeners, err := getInsecureListeners(conf.SocketPath, conf.ListenAddr)
	if err != nil {
		logger.Fatalf("%s", err)
	}

	if err := run(listeners, conf); err != nil {
		logger.Fatalf("%v", err)
	}
}

func configure() (config.Config, error) {
	var conf config.Config

	if *flagConfig == "" {
		return conf, errNoConfigFile
	}

	conf, err := config.FromFile(*flagConfig)
	if err != nil {
		return conf, fmt.Errorf("error reading config file: %v", err)
	}

	if err := conf.Validate(); err != nil {
		return conf, err
	}

	logger = conf.ConfigureLogger()
	tracing.Initialize(tracing.WithServiceName("praefect"))

	if conf.PrometheusListenAddr != "" {
		logger.WithField("address", conf.PrometheusListenAddr).Info("Starting prometheus listener")
		promMux := http.NewServeMux()
		promMux.Handle("/metrics", promhttp.Handler())

		go func() {
			http.ListenAndServe(conf.PrometheusListenAddr, promMux)
		}()
	}

	registerServerVersionPromGauge()
	logger.WithField("version", praefect.GetVersionString()).Info("Starting Praefect")

	return conf, nil
}

func run(listeners []net.Listener, conf config.Config) error {

	var (
		// top level server dependencies
		datastore   = praefect.NewMemoryDatastore(conf)
		coordinator = praefect.NewCoordinator(logger, datastore, protoregistry.GitalyProtoFileDescriptors...)
		repl        = praefect.NewReplMgr("default", logger, datastore, coordinator)
		srv         = praefect.NewServer(coordinator, repl, nil, logger)
		// signal related
		signals      = []os.Signal{syscall.SIGTERM, syscall.SIGINT}
		termCh       = make(chan os.Signal, len(signals))
		serverErrors = make(chan error, 1)
	)

	signal.Notify(termCh, signals...)

	servers := []*praefect.Server{srv}

	for _, l := range listeners {
		go func(lis net.Listener) { serverErrors <- srv.Start(lis) }(l)
	}

	if conf.TLSListenAddr != "" {
		cert, err := tls.LoadX509KeyPair(conf.TLS.CertificatePath, conf.TLS.KeyPath)
		if err != nil {
			logger.Fatal(err)
		}

		secureSrv := praefect.NewServer(coordinator, repl, []grpc.ServerOption{grpc.Creds(credentials.NewServerTLSFromCert(&cert))}, logger)

		secureListener, err := getSecureListener(conf.TLSListenAddr)
		if err != nil {
			logger.Fatal(err)
		}
		go func() { serverErrors <- secureSrv.Start(secureListener) }()

		servers = append(servers, secureSrv)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, node := range conf.Nodes {
		if err := coordinator.RegisterNode(node.Storage, node.Address); err != nil {
			return fmt.Errorf("failed to register %s: %s", node.Address, err)
		}

		logger.WithField("node_address", node.Address).Info("registered gitaly node")
	}

	go func() { serverErrors <- repl.ProcessBacklog(ctx) }()

	go coordinator.FailoverRotation()

	select {
	case s := <-termCh:
		logger.WithField("signal", s).Warn("received signal, shutting down gracefully")
		cancel() // cancels the replicator job processing

		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

		g, ctx := errgroup.WithContext(ctx)

		for _, srv := range servers {
			g.Go(func() error {
				if shutdownErr := srv.Shutdown(ctx); shutdownErr != nil {
					logger.Warnf("error received during shutting down: %v", shutdownErr)
					return shutdownErr
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}
	case err := <-serverErrors:
		return err
	}

	return nil
}

func getInsecureListeners(socketPath, listenAddr string) ([]net.Listener, error) {
	var listeners []net.Listener

	if socketPath != "" {
		if err := os.RemoveAll(socketPath); err != nil {
			return nil, err
		}

		cleanPath := strings.TrimPrefix(socketPath, "unix:")
		l, err := net.Listen("unix", cleanPath)
		if err != nil {
			return nil, err
		}

		listeners = append(listeners, l)

		logger.WithField("address", socketPath).Info("listening on unix socket")
	}

	if listenAddr != "" {
		l, err := net.Listen("tcp", listenAddr)
		if err != nil {
			return nil, err
		}

		listeners = append(listeners, l)
		logger.WithField("address", listenAddr).Info("listening at tcp address")
	}

	return listeners, nil
}

func getSecureListener(tlsListenAddr string) (net.Listener, error) {
	if tlsListenAddr != "" {
		l, err := net.Listen("tcp", tlsListenAddr)
		if err != nil {
			return nil, err
		}

		logger.WithField("address", tlsListenAddr).Info("listening at tcp address")
		return l, nil
	}

	return nil, errors.New("listen address empty")
}

// registerServerVersionPromGauge registers a label with the current server version
// making it easy to see what versions of Gitaly are running across a cluster
func registerServerVersionPromGauge() {
	gitlabBuildInfoGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gitlab_build_info",
		Help: "Current build info for this GitLab Service",
		ConstLabels: prometheus.Labels{
			"version": praefect.GetVersion(),
			"built":   praefect.GetBuildTime(),
		},
	})

	prometheus.MustRegister(gitlabBuildInfoGauge)
	gitlabBuildInfoGauge.Set(1)
}
