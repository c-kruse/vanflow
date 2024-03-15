package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/c-kruse/vanflow/session"
	"github.com/cenkalti/backoff/v4"
)

var (
	flags           *flag.FlagSet = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	MessagingConfig string
	AmqpServer      string
	TLSVerify       bool
	TLSCACert       string
	TLSCert         string
	TLSKey          string

	FlushOnDiscover   bool
	EnableHeartbeat   bool
	EnableRecords     bool
	EnableRecordsFlow bool
	EnableRecordsLogs bool
	Debug             bool
)

func init() {
	flags.Usage = func() {
		fmt.Printf(`Usage of %s:

Commands:
		log - log messages to stdout
		serve - collect and store records and serve them over http
		fixture - listens for a set of records over http, then impersonates
				the event sources described in that record set
`, os.Args[0])
		flags.PrintDefaults()
	}
	flags.BoolVar(&Debug, "debug", false, "enalbe debug logging")
	flags.StringVar(&MessagingConfig, "messaging-config", "", "optional path to a skupper connect.json")
	flags.StringVar(&AmqpServer, "server", "amqp://localhost:5671", "AMQP server to connect to")
	flags.BoolVar(&TLSVerify, "tls-verify", true, "validate server CA")
	flags.StringVar(&TLSCACert, "ca", "", "path to AMQP CA certificate")
	flags.StringVar(&TLSCert, "cert", "", "path to certificate when connecting with amqps")
	flags.StringVar(&TLSKey, "key", "", "path to certificate key when connecting with amqps")

	flags.BoolVar(&FlushOnDiscover, "flush-on-discover", false, "enable to send a FLUSH to newly discovered event sources when in log only mode")
	flags.BoolVar(&EnableHeartbeat, "enable-heartbeats", false, "log observed heartbeat messages when in log only mode")
	flags.BoolVar(&EnableRecords, "enable-records", true, "logs observed record messages when in log only mode")

	flags.BoolVar(&EnableRecordsFlow, "include-flow-records", true, "include flow records")
	flags.BoolVar(&EnableRecordsLogs, "include-log-records", true, "include log records")
	flags.Parse(os.Args[1:])
}
func main() {
	if len(flags.Args()) != 1 {
		fmt.Printf("error: expected command. got %v\n", flags.Args())
		flags.Usage()
		os.Exit(1)
	}

	if Debug {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
		slog.SetDefault(logger)
	}

	connURL := AmqpServer

	var tlsCfg *tls.Config
	if MessagingConfig != "" {
		b, err := os.ReadFile(MessagingConfig)
		if err != nil {
			fmt.Printf("error: could not read messaging-config %s\n", err)
			flags.Usage()
			os.Exit(1)
		}
		var cfg connectJSON
		if err := json.Unmarshal(b, &cfg); err != nil {
			fmt.Printf("error: could not parse messaging-config %s\n", err)
			flags.Usage()
			os.Exit(1)
		}
		if cfg.Tls.CA != "" {
			tlsCfg, err = cfg.Tls.Parse()
			if err != nil {
				fmt.Printf("error: could not parse tls config in messaging-config %s\n", err)
				flags.Usage()
				os.Exit(1)
			}
		}
		connURL = fmt.Sprintf("%s://%s:%s", cfg.Scheme, cfg.Host, cfg.Port)
	}

	if TLSCert != "" {
		var err error
		tlsCfg, err = tlsConfig{
			CA:     TLSCACert,
			Cert:   TLSCert,
			Key:    TLSKey,
			Verify: TLSVerify,
		}.Parse()
		if err != nil {
			fmt.Printf("error: could not parse tls config from tls flags %s\n", err)
			flags.Usage()
			os.Exit(1)
		}
	}

	var sasl session.SASLType
	if tlsCfg != nil {
		sasl = session.SASLTypeExternal
	}

	containerBackoff := backoff.NewExponentialBackOff()
	containerBackoff.MaxElapsedTime = 60 * time.Second
	factory := session.NewContainerFactory(connURL, session.ContainerConfig{
		TLSConfig: tlsCfg, SASLType: sasl,
		BackOff: containerBackoff,
	})

	var cmdHandler func(context.Context, session.ContainerFactory)
	switch name := flags.Arg(0); name {
	case "log":
		cmdHandler = logOnly
	case "serve":
		cmdHandler = serveRecords
	case "fixture":
		cmdHandler = serveFixture
	default:
		fmt.Printf("error: unexpected command %s\n", name)
		flags.Usage()
		os.Exit(1)

	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	cmdHandler(ctx, factory)
}

type tlsConfig struct {
	CA     string `json:"ca,omitempty"`
	Cert   string `json:"cert,omitempty"`
	Key    string `json:"key,omitempty"`
	Verify bool   `json:"verify,omitempty"`
}

type connectJSON struct {
	Scheme string    `json:"scheme,omitempty"`
	Host   string    `json:"host,omitempty"`
	Port   string    `json:"port,omitempty"`
	Tls    tlsConfig `json:"tls,omitempty"`
}

func (c tlsConfig) Parse() (*tls.Config, error) {
	var config tls.Config
	config.InsecureSkipVerify = true
	if c.Verify {
		certPool := x509.NewCertPool()
		file, err := os.ReadFile(c.CA)
		if err != nil {
			return nil, err
		}
		certPool.AppendCertsFromPEM(file)
		config.RootCAs = certPool
		config.InsecureSkipVerify = false
	}

	_, errCert := os.Stat(c.Cert)
	_, errKey := os.Stat(c.Key)
	if errCert == nil || errKey == nil {
		tlsCert, err := tls.LoadX509KeyPair(c.Cert, c.Key)
		if err != nil {
			return nil, fmt.Errorf("could not load x509 key pair: %v", err)
		}
		config.Certificates = []tls.Certificate{tlsCert}
	}
	config.MinVersion = tls.VersionTLS10
	return &config, nil
}
