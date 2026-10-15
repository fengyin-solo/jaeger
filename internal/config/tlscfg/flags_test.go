// Copyright (c) 2019 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tlscfg

import (
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"

	"github.com/jaegertracing/jaeger/internal/config"
)

func TestClientFlags(t *testing.T) {
	cmdLine := []string{
		"--prefix.tls.ca=ca-file",
		"--prefix.tls.cert=cert-file",
		"--prefix.tls.key=key-file",
		"--prefix.tls.server-name=HAL1",
		"--prefix.tls.skip-host-verify=true",
	}

	tests := []struct {
		option string
	}{
		{
			option: "--prefix.tls.enabled=true",
		},
	}

	for _, test := range tests {
		t.Run(test.option, func(t *testing.T) {
			v := viper.New()
			command := cobra.Command{}
			flagSet := &flag.FlagSet{}
			flagCfg := ClientFlagsConfig{
				Prefix: "prefix",
			}
			flagCfg.AddFlags(flagSet)
			command.PersistentFlags().AddGoFlagSet(flagSet)
			v.BindPFlags(command.PersistentFlags())

			err := command.ParseFlags(append(cmdLine, test.option))
			require.NoError(t, err)
			clientCfg, err := flagCfg.InitFromViper(v)
			require.NoError(t, err)

			expectedCfg := configtls.ClientConfig{
				Config: configtls.Config{
					CAFile:                   "ca-file",
					CertFile:                 "cert-file",
					KeyFile:                  "key-file",
					IncludeSystemCACertsPool: false,
					CipherSuites:             nil,
					ReloadInterval:           0,
				},
				Insecure:           false,
				InsecureSkipVerify: true,
				ServerName:         "HAL1",
			}

			assert.Equal(t, expectedCfg, clientCfg)
		})
	}
}

func TestServerFlags(t *testing.T) {
	cmdLine := []string{
		"##placeholder##", // replaced in each test below
		"--prefix.tls.enabled=true",
		"--prefix.tls.cert=cert-file",
		"--prefix.tls.key=key-file",
		"--prefix.tls.cipher-suites=TLS_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
		"--prefix.tls.min-version=1.2",
		"--prefix.tls.max-version=1.3",
	}

	tests := []struct {
		option string
		file   string
	}{
		{
			option: "--prefix.tls.client-ca=client-ca-file",
			file:   "client-ca-file",
		},
	}

	for _, test := range tests {
		t.Run(test.file, func(t *testing.T) {
			v := viper.New()
			command := cobra.Command{}
			flagSet := &flag.FlagSet{}
			flagCfg := ServerFlagsConfig{
				Prefix: "prefix",
			}
			flagCfg.AddFlags(flagSet)
			command.PersistentFlags().AddGoFlagSet(flagSet)
			v.BindPFlags(command.PersistentFlags())

			cmdLine[0] = test.option
			err := command.ParseFlags(cmdLine)
			require.NoError(t, err)
			serverConfig, err := flagCfg.InitFromViper(v)
			require.NoError(t, err)

			expectedConfig := configoptional.Some(configtls.ServerConfig{
				ClientCAFile: "client-ca-file",
				Config: configtls.Config{
					CertFile:     "cert-file",
					KeyFile:      "key-file",
					MinVersion:   "1.2",
					MaxVersion:   "1.3",
					CipherSuites: []string{"TLS_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"},
				},
			})

			assert.Equal(t, expectedConfig, serverConfig)
		})
	}
}

func TestServerCertReloadInterval(t *testing.T) {
	cfg := ServerFlagsConfig{
		Prefix: "prefix",
	}
	v, command := config.Viperize(cfg.AddFlags)
	err := command.ParseFlags([]string{
		"--prefix.tls.enabled=true",
		"--prefix.tls.reload-interval=24h",
	})
	require.NoError(t, err)
	tlscfg, err := cfg.InitFromViper(v)
	require.NoError(t, err)
	require.True(t, tlscfg.HasValue())
	assert.Equal(t, 24*time.Hour, tlscfg.Get().ReloadInterval)
}

// TestFailedTLSFlags verifies that TLS options cannot be used when tls.enabled=false
func TestFailedTLSFlags(t *testing.T) {
	clientTests := [][]string{
		{".ca=blah"},
		{".cert=blah", ".key=blah"},
		{".server-name=blah"},
		{".skip-host-verify=true"},
	}
	serverTests := [][]string{
		{".cert=blah", ".key=blah"},
		{".client-ca=blah"},
		{".cipher-suites=TLS_AES_256_GCM_SHA384"},
		{".min-version=1.1"},
		{".max-version=1.3"},
		{".reload-interval=1s"},
	}

	allTests := []struct {
		side  string
		tests [][]string
	}{
		{side: "client", tests: clientTests},
		{side: "server", tests: serverTests},
	}

	for _, metaTest := range allTests {
		t.Run(metaTest.side, func(t *testing.T) {
			for _, testFlags := range metaTest.tests {
				t.Run(strings.Join(testFlags, ","), func(t *testing.T) {
					var (
						addFlags      func(*flag.FlagSet)
						initFromViper func(*viper.Viper) (any, error)
					)

					if metaTest.side == "client" {
						clientConfig := &ClientFlagsConfig{Prefix: "prefix"}
						addFlags = clientConfig.AddFlags
						initFromViper = func(v *viper.Viper) (any, error) {
							return clientConfig.InitFromViper(v)
						}
					} else {
						serverConfig := &ServerFlagsConfig{Prefix: "prefix"}
						addFlags = serverConfig.AddFlags
						initFromViper = func(v *viper.Viper) (any, error) {
							return serverConfig.InitFromViper(v)
						}
					}

					v, command := config.Viperize(addFlags)

					toCmdLine := func(enabled bool) []string {
						cmdLine := []string{"--prefix.tls.enabled=false"}
						if enabled {
							cmdLine[0] = "--prefix.tls.enabled=true"
						}
						for _, f := range testFlags {
							cmdLine = append(cmdLine, "--prefix.tls"+f)
						}
						return cmdLine
					}

					// When enabled, the options are valid and no error is returned.
					err := command.ParseFlags(toCmdLine(true))
					require.NoError(t, err)
					result, err := initFromViper(v)
					require.NoError(t, err)

					switch metaTest.side {
					case "client":
						require.IsType(t, configtls.ClientConfig{}, result, "result should be of type configtls.ClientConfig")
					case "server":
						exp := configoptional.Some(configtls.ServerConfig{})
						require.IsType(t, exp, result, "result should be of type configoptional.Optional[configtls.ServerConfig]")
					default:
						t.Errorf("Unexpected side value: %s", metaTest.side)
					}

					// When disabled, the same options are rejected.
					err = command.ParseFlags(toCmdLine(false))
					require.NoError(t, err)
					_, err = initFromViper(v)
					require.Error(t, err)
					require.EqualError(t, err, "prefix.tls.* options cannot be used when prefix.tls.enabled is false")
				})
			}
		})
	}
}

func TestValidateClientFlagsFromViper(t *testing.T) {
	testCases := []struct {
		name      string
		setFlags  func(*flag.FlagSet)
		cmdLine   []string
		wantError string
	}{
		{
			name:     "cert without key",
			setFlags: ClientFlagsConfig{Prefix: "prefix"}.AddFlags,
			cmdLine: []string{
				"--prefix.tls.enabled=true",
				"--prefix.tls.cert=cert-file",
			},
			wantError: "prefix.tls.cert is set but prefix.tls.key is not: " +
				"the two options must be provided together",
		},
		{
			name:     "key without cert",
			setFlags: ClientFlagsConfig{Prefix: "prefix"}.AddFlags,
			cmdLine: []string{
				"--prefix.tls.enabled=true",
				"--prefix.tls.key=key-file",
			},
			wantError: "prefix.tls.key is set but prefix.tls.cert is not: " +
				"the two options must be provided together",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v, command := config.Viperize(tc.setFlags)
			require.NoError(t, command.ParseFlags(tc.cmdLine))
			_, err := ClientFlagsConfig{Prefix: "prefix"}.InitFromViper(v)
			require.EqualError(t, err, tc.wantError)
		})
	}
}

func TestValidateServerFlagsFromViper(t *testing.T) {
	testCases := []struct {
		name      string
		cmdLine   []string
		wantError string
	}{
		{
			name: "cert without key",
			cmdLine: []string{
				"--prefix.tls.enabled=true",
				"--prefix.tls.cert=cert-file",
			},
			wantError: "prefix.tls.cert is set but prefix.tls.key is not: " +
				"the two options must be provided together",
		},
		{
			name: "invalid min-version",
			cmdLine: []string{
				"--prefix.tls.enabled=true",
				"--prefix.tls.min-version=TLS1.2",
			},
			wantError: `prefix.tls.min-version: invalid value "TLS1.2", ` +
				"supported values are 1.0, 1.1, 1.2, 1.3",
		},
		{
			name: "invalid max-version",
			cmdLine: []string{
				"--prefix.tls.enabled=true",
				"--prefix.tls.max-version=1.4",
			},
			wantError: `prefix.tls.max-version: invalid value "1.4", ` +
				"supported values are 1.0, 1.1, 1.2, 1.3",
		},
		{
			name: "min-version greater than max-version",
			cmdLine: []string{
				"--prefix.tls.enabled=true",
				"--prefix.tls.min-version=1.3",
				"--prefix.tls.max-version=1.2",
			},
			wantError: "prefix.tls.min-version (1.3) must not be greater than prefix.tls.max-version (1.2)",
		},
		{
			name: "unknown cipher suite",
			cmdLine: []string{
				"--prefix.tls.enabled=true",
				"--prefix.tls.cipher-suites=TLS_AES_256_GCM_SHA384, NOT_A_CIPHER",
			},
			wantError: `prefix.tls.cipher-suites: "NOT_A_CIPHER" is not a valid cipher suite name, ` +
				"see https://golang.org/pkg/crypto/tls/#pkg-constants",
		},
		{
			name: "negative reload-interval",
			cmdLine: []string{
				"--prefix.tls.enabled=true",
				"--prefix.tls.reload-interval=-1s",
			},
			wantError: "prefix.tls.reload-interval must not be negative, got -1s",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ServerFlagsConfig{Prefix: "prefix"}
			v, command := config.Viperize(cfg.AddFlags)
			require.NoError(t, command.ParseFlags(tc.cmdLine))
			_, err := cfg.InitFromViper(v)
			require.EqualError(t, err, tc.wantError)
		})
	}
}

// TestTLSFlagsSideMixing verifies that options belonging to the other side of
// the TLS connection are rejected when they come from a configuration file
// rather than a registered command line flag.
func TestTLSFlagsSideMixing(t *testing.T) {
	t.Run("server options in client config", func(t *testing.T) {
		v, _ := config.Viperize(ClientFlagsConfig{Prefix: "prefix"}.AddFlags)
		v.Set("prefix.tls.enabled", true)
		v.Set("prefix.tls.client-ca", "client-ca-file")
		_, err := ClientFlagsConfig{Prefix: "prefix"}.InitFromViper(v)
		require.EqualError(t, err,
			"prefix.tls.client-ca is a server-only TLS option and cannot be set in the client TLS configuration")
	})

	t.Run("client options in server config", func(t *testing.T) {
		v, _ := config.Viperize(ServerFlagsConfig{Prefix: "prefix"}.AddFlags)
		v.Set("prefix.tls.enabled", true)
		v.Set("prefix.tls.server-name", "example.com")
		_, err := ServerFlagsConfig{Prefix: "prefix"}.InitFromViper(v)
		require.EqualError(t, err,
			"prefix.tls.server-name is a client-only TLS option and cannot be set in the server TLS configuration")
	})
}
