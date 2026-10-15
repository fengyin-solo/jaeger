// Copyright (c) 2019 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tlscfg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"
)

func TestToOtelClientConfig(t *testing.T) {
	testCases := []struct {
		name     string
		options  options
		expected configtls.ClientConfig
	}{
		{
			name: "insecure",
			options: options{
				Enabled: false,
			},
			expected: configtls.ClientConfig{
				Insecure: true,
			},
		},
		{
			name: "secure with skip host verify",
			options: options{
				Enabled:        true,
				SkipHostVerify: true,
				ServerName:     "example.com",
				CAPath:         "path/to/ca.pem",
				CertPath:       "path/to/cert.pem",
				KeyPath:        "path/to/key.pem",
				CipherSuites:   []string{"TLS_RSA_WITH_AES_128_CBC_SHA"},
				MinVersion:     "1.2",
				MaxVersion:     "1.3",
				ReloadInterval: 24 * time.Hour,
			},
			expected: configtls.ClientConfig{
				Insecure:           false,
				InsecureSkipVerify: true,
				ServerName:         "example.com",
				Config: configtls.Config{
					CAFile:         "path/to/ca.pem",
					CertFile:       "path/to/cert.pem",
					KeyFile:        "path/to/key.pem",
					CipherSuites:   []string{"TLS_RSA_WITH_AES_128_CBC_SHA"},
					MinVersion:     "1.2",
					MaxVersion:     "1.3",
					ReloadInterval: 24 * time.Hour,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.options.ToOtelClientConfig()
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestToOtelServerConfig(t *testing.T) {
	testCases := []struct {
		name     string
		options  options
		expected configoptional.Optional[configtls.ServerConfig]
	}{
		{
			name: "not enabled",
			options: options{
				Enabled: false,
			},
			expected: configoptional.None[configtls.ServerConfig](),
		},
		{
			name: "default mapping",
			options: options{
				Enabled:      true,
				ClientCAPath: "path/to/client/ca.pem",
				CAPath:       "path/to/ca.pem",
				CertPath:     "path/to/cert.pem",
				KeyPath:      "path/to/key.pem",
				CipherSuites: []string{"TLS_RSA_WITH_AES_128_CBC_SHA"},
				MinVersion:   "1.2",
				MaxVersion:   "1.3",
			},
			expected: configoptional.Some(configtls.ServerConfig{
				ClientCAFile: "path/to/client/ca.pem",
				Config: configtls.Config{
					CAFile:       "path/to/ca.pem",
					CertFile:     "path/to/cert.pem",
					KeyFile:      "path/to/key.pem",
					CipherSuites: []string{"TLS_RSA_WITH_AES_128_CBC_SHA"},
					MinVersion:   "1.2",
					MaxVersion:   "1.3",
				},
			}),
		},
		{
			name: "with reload interval",
			options: options{
				Enabled:        true,
				ClientCAPath:   "path/to/client/ca.pem",
				CAPath:         "path/to/ca.pem",
				CertPath:       "path/to/cert.pem",
				KeyPath:        "path/to/key.pem",
				CipherSuites:   []string{"TLS_RSA_WITH_AES_128_CBC_SHA"},
				MinVersion:     "1.2",
				MaxVersion:     "1.3",
				ReloadInterval: 24 * time.Hour,
			},
			expected: configoptional.Some(configtls.ServerConfig{
				ClientCAFile:       "path/to/client/ca.pem",
				ReloadClientCAFile: true,
				Config: configtls.Config{
					CAFile:         "path/to/ca.pem",
					CertFile:       "path/to/cert.pem",
					KeyFile:        "path/to/key.pem",
					CipherSuites:   []string{"TLS_RSA_WITH_AES_128_CBC_SHA"},
					MinVersion:     "1.2",
					MaxVersion:     "1.3",
					ReloadInterval: 24 * time.Hour,
				},
			}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.options.ToOtelServerConfig()
			assert.Equal(t, tc.expected, actual)
		})
	}
}

const validCipherSuite = "TLS_AES_256_GCM_SHA384"

func TestValidate(t *testing.T) {
	testCases := []struct {
		name      string
		side      tlsSide
		options   options
		wantError string
	}{
		// Valid configurations.
		{
			name:    "client with no options",
			side:    clientSide,
			options: options{Enabled: true},
		},
		{
			name: "client with all client-side options",
			side: clientSide,
			options: options{
				Enabled:        true,
				CAPath:         "ca.pem",
				CertPath:       "cert.pem",
				KeyPath:        "key.pem",
				ServerName:     "example.com",
				SkipHostVerify: true,
			},
		},
		{
			name: "server with required options",
			side: serverSide,
			options: options{
				Enabled:      true,
				CertPath:     "cert.pem",
				KeyPath:      "key.pem",
				ClientCAPath: "client-ca.pem",
			},
		},
		{
			name: "server with all server-side options",
			side: serverSide,
			options: options{
				Enabled:        true,
				CertPath:       "cert.pem",
				KeyPath:        "key.pem",
				ClientCAPath:   "client-ca.pem",
				CipherSuites:   []string{validCipherSuite},
				MinVersion:     "1.2",
				MaxVersion:     "1.3",
				ReloadInterval: 24 * time.Hour,
			},
		},

		// Certificate / key pairing.
		{
			name: "client cert without key",
			side: clientSide,
			options: options{
				Enabled:  true,
				CertPath: "cert.pem",
			},
			wantError: "prefix.tls.cert is set but prefix.tls.key is not: " +
				"the two options must be provided together",
		},
		{
			name: "client key without cert",
			side: clientSide,
			options: options{
				Enabled: true,
				KeyPath: "key.pem",
			},
			wantError: "prefix.tls.key is set but prefix.tls.cert is not: " +
				"the two options must be provided together",
		},
		{
			name: "server cert without key",
			side: serverSide,
			options: options{
				Enabled:  true,
				CertPath: "cert.pem",
			},
			wantError: "prefix.tls.cert is set but prefix.tls.key is not: " +
				"the two options must be provided together",
		},
		{
			name: "server key without cert",
			side: serverSide,
			options: options{
				Enabled: true,
				KeyPath: "key.pem",
			},
			wantError: "prefix.tls.key is set but prefix.tls.cert is not: " +
				"the two options must be provided together",
		},

		// TLS versions.
		{
			name: "invalid min-version",
			side: serverSide,
			options: options{
				Enabled:    true,
				CertPath:   "cert.pem",
				KeyPath:    "key.pem",
				MinVersion: "TLSv1.2",
			},
			wantError: `prefix.tls.min-version: invalid value "TLSv1.2", ` +
				"supported values are 1.0, 1.1, 1.2, 1.3",
		},
		{
			name: "invalid max-version",
			side: serverSide,
			options: options{
				Enabled:    true,
				CertPath:   "cert.pem",
				KeyPath:    "key.pem",
				MaxVersion: "1.4",
			},
			wantError: `prefix.tls.max-version: invalid value "1.4", ` +
				"supported values are 1.0, 1.1, 1.2, 1.3",
		},
		{
			name: "min-version greater than max-version",
			side: serverSide,
			options: options{
				Enabled:    true,
				CertPath:   "cert.pem",
				KeyPath:    "key.pem",
				MinVersion: "1.3",
				MaxVersion: "1.2",
			},
			wantError: "prefix.tls.min-version (1.3) must not be greater than " +
				"prefix.tls.max-version (1.2)",
		},

		// Cipher suites.
		{
			name: "unknown cipher suite",
			side: serverSide,
			options: options{
				Enabled:      true,
				CertPath:     "cert.pem",
				KeyPath:      "key.pem",
				CipherSuites: []string{"TLS_NOT_A_CIPHER_SUITE"},
			},
			wantError: `prefix.tls.cipher-suites: "TLS_NOT_A_CIPHER_SUITE" is not a valid ` +
				"cipher suite name, see https://golang.org/pkg/crypto/tls/#pkg-constants",
		},
		{
			name: "second cipher suite unknown",
			side: serverSide,
			options: options{
				Enabled:  true,
				CertPath: "cert.pem",
				KeyPath:  "key.pem",
				CipherSuites: []string{
					validCipherSuite,
					"TLS_NOT_A_CIPHER_SUITE",
				},
			},
			wantError: `prefix.tls.cipher-suites: "TLS_NOT_A_CIPHER_SUITE" is not a valid ` +
				"cipher suite name, see https://golang.org/pkg/crypto/tls/#pkg-constants",
		},

		// Client / server option mixing.
		{
			name: "client-ca on client config",
			side: clientSide,
			options: options{
				Enabled:      true,
				ClientCAPath: "client-ca.pem",
			},
			wantError: "prefix.tls.client-ca is a server-only TLS option and " +
				"cannot be set in the client TLS configuration",
		},
		{
			name: "cipher-suites on client config",
			side: clientSide,
			options: options{
				Enabled:      true,
				CipherSuites: []string{validCipherSuite},
			},
			wantError: "prefix.tls.cipher-suites is a server-only TLS option and " +
				"cannot be set in the client TLS configuration",
		},
		{
			name: "min-version on client config",
			side: clientSide,
			options: options{
				Enabled:    true,
				MinVersion: "1.2",
			},
			wantError: "prefix.tls.min-version is a server-only TLS option and " +
				"cannot be set in the client TLS configuration",
		},
		{
			name: "max-version on client config",
			side: clientSide,
			options: options{
				Enabled:    true,
				MaxVersion: "1.3",
			},
			wantError: "prefix.tls.max-version is a server-only TLS option and " +
				"cannot be set in the client TLS configuration",
		},
		{
			name: "reload-interval on client config",
			side: clientSide,
			options: options{
				Enabled:        true,
				ReloadInterval: time.Hour,
			},
			wantError: "prefix.tls.reload-interval is a server-only TLS option and " +
				"cannot be set in the client TLS configuration",
		},
		{
			name: "ca on server config",
			side: serverSide,
			options: options{
				Enabled: true,
				CAPath:  "ca.pem",
			},
			wantError: "prefix.tls.ca is a client-only TLS option and " +
				"cannot be set in the server TLS configuration",
		},
		{
			name: "server-name on server config",
			side: serverSide,
			options: options{
				Enabled:    true,
				ServerName: "example.com",
			},
			wantError: "prefix.tls.server-name is a client-only TLS option and " +
				"cannot be set in the server TLS configuration",
		},
		{
			name: "skip-host-verify on server config",
			side: serverSide,
			options: options{
				Enabled:        true,
				SkipHostVerify: true,
			},
			wantError: "prefix.tls.skip-host-verify is a client-only TLS option and " +
				"cannot be set in the server TLS configuration",
		},

		// Reload interval.
		{
			name: "negative reload-interval",
			side: serverSide,
			options: options{
				Enabled:        true,
				CertPath:       "cert.pem",
				KeyPath:        "key.pem",
				ReloadInterval: -time.Second,
			},
			wantError: "prefix.tls.reload-interval must not be negative, got -1s",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.options.validate(tc.side, "prefix")
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantError)
			}
		})
	}
}
