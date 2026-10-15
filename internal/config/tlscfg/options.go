// Copyright (c) 2019 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tlscfg

import (
	"crypto/tls"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"
)

// options describes the configuration properties for TLS Connections.
type options struct {
	Enabled        bool
	CAPath         string // only for client-side TLS config
	CertPath       string
	KeyPath        string
	ServerName     string        // only for client-side TLS config
	ClientCAPath   string        // only for server-side TLS config for client auth
	CipherSuites   []string      // only for server-side TLS config
	MinVersion     string        // only for server-side TLS config
	MaxVersion     string        // only for server-side TLS config
	SkipHostVerify bool          // only for client-side TLS config
	ReloadInterval time.Duration // only for server-side TLS config
}

// tlsSide identifies which side of a TLS connection the options configure.
type tlsSide int

const (
	clientSide tlsSide = iota
	serverSide
)

// supportedTLSVersions maps the accepted --*.tls.min/max-version values to
// the corresponding crypto/tls constants.
var supportedTLSVersions = map[string]uint16{
	"1.0": tls.VersionTLS10,
	"1.1": tls.VersionTLS11,
	"1.2": tls.VersionTLS12,
	"1.3": tls.VersionTLS13,
}

// supportedTLSVersionValues lists supportedTLSVersions keys for error messages.
const supportedTLSVersionValues = "1.0, 1.1, 1.2, 1.3"

// supportedCipherSuites contains the names of the cipher suites returned by
// tls.CipherSuites(), i.e. the secure suites that configtls accepts by default.
var supportedCipherSuites = func() map[string]struct{} {
	suites := make(map[string]struct{})
	for _, suite := range tls.CipherSuites() {
		suites[suite.Name] = struct{}{}
	}
	return suites
}()

// validate checks the options before they are converted into configtls
// configuration, so that configuration mistakes are reported with the flag
// name rather than failing later during the TLS handshake. prefix is the flag
// prefix (e.g. "collector.grpc") used to build the full option name shown in
// the error messages.
func (o *options) validate(side tlsSide, prefix string) error {
	if err := o.validateSide(side, prefix); err != nil {
		return err
	}
	if err := o.validateCertAndKey(prefix); err != nil {
		return err
	}
	if err := o.validateVersions(prefix); err != nil {
		return err
	}
	if err := o.validateCipherSuites(prefix); err != nil {
		return err
	}
	if o.ReloadInterval < 0 {
		return fmt.Errorf(
			"%s must not be negative, got %s",
			prefix+tlsReloadInterval, o.ReloadInterval)
	}
	return nil
}

// validateSide rejects options that belong to the other side of the TLS
// connection (e.g. client CA on a client configuration).
func (o *options) validateSide(side tlsSide, prefix string) error {
	if side == clientSide {
		if o.ClientCAPath != "" {
			return serverOnlyOptionError(prefix, tlsClientCA)
		}
		if len(o.CipherSuites) > 0 {
			return serverOnlyOptionError(prefix, tlsCipherSuites)
		}
		if o.MinVersion != "" {
			return serverOnlyOptionError(prefix, tlsMinVersion)
		}
		if o.MaxVersion != "" {
			return serverOnlyOptionError(prefix, tlsMaxVersion)
		}
		if o.ReloadInterval != 0 {
			return serverOnlyOptionError(prefix, tlsReloadInterval)
		}
		return nil
	}

	if o.CAPath != "" {
		return clientOnlyOptionError(prefix, tlsCA)
	}
	if o.ServerName != "" {
		return clientOnlyOptionError(prefix, tlsServerName)
	}
	if o.SkipHostVerify {
		return clientOnlyOptionError(prefix, tlsSkipHostVerify)
	}
	return nil
}

// validateCertAndKey ensures the certificate and the private key are provided
// together.
func (o *options) validateCertAndKey(prefix string) error {
	if o.CertPath != "" && o.KeyPath == "" {
		return fmt.Errorf(
			"%s is set but %s is not: the two options must be provided together",
			prefix+tlsCert, prefix+tlsKey)
	}
	if o.CertPath == "" && o.KeyPath != "" {
		return fmt.Errorf(
			"%s is set but %s is not: the two options must be provided together",
			prefix+tlsKey, prefix+tlsCert)
	}
	return nil
}

// validateVersions checks the TLS version values and ensures the minimum
// version does not exceed the maximum version.
func (o *options) validateVersions(prefix string) error {
	minVersion, err := parseTLSVersion(o.MinVersion, prefix+tlsMinVersion)
	if err != nil {
		return err
	}
	maxVersion, err := parseTLSVersion(o.MaxVersion, prefix+tlsMaxVersion)
	if err != nil {
		return err
	}
	if o.MinVersion != "" && o.MaxVersion != "" && minVersion > maxVersion {
		return fmt.Errorf(
			"%s (%s) must not be greater than %s (%s)",
			prefix+tlsMinVersion, o.MinVersion, prefix+tlsMaxVersion, o.MaxVersion)
	}
	return nil
}

// parseTLSVersion converts a version string to a crypto/tls version constant.
// An empty string means the version is not set and is left to the default.
func parseTLSVersion(version, option string) (uint16, error) {
	if version == "" {
		return 0, nil
	}
	v, ok := supportedTLSVersions[version]
	if !ok {
		return 0, fmt.Errorf(
			"%s: invalid value %q, supported values are %s",
			option, version, supportedTLSVersionValues)
	}
	return v, nil
}

// validateCipherSuites ensures every cipher suite name resolves to a Go
// crypto/tls cipher suite constant.
func (o *options) validateCipherSuites(prefix string) error {
	for _, suite := range o.CipherSuites {
		if _, ok := supportedCipherSuites[suite]; !ok {
			return fmt.Errorf(
				"%s: %q is not a valid cipher suite name, see https://golang.org/pkg/crypto/tls/#pkg-constants",
				prefix+tlsCipherSuites, suite)
		}
	}
	return nil
}

func serverOnlyOptionError(prefix, option string) error {
	return fmt.Errorf(
		"%s is a server-only TLS option and cannot be set in the client TLS configuration",
		prefix+option)
}

func clientOnlyOptionError(prefix, option string) error {
	return fmt.Errorf(
		"%s is a client-only TLS option and cannot be set in the server TLS configuration",
		prefix+option)
}

func (o *options) ToOtelClientConfig() configtls.ClientConfig {
	return configtls.ClientConfig{
		Insecure:           !o.Enabled,
		InsecureSkipVerify: o.SkipHostVerify,
		ServerName:         o.ServerName,
		Config: configtls.Config{
			CAFile:         o.CAPath,
			CertFile:       o.CertPath,
			KeyFile:        o.KeyPath,
			CipherSuites:   o.CipherSuites,
			MinVersion:     o.MinVersion,
			MaxVersion:     o.MaxVersion,
			ReloadInterval: o.ReloadInterval,

			// when no truststore given, use SystemCertPool
			// https://github.com/jaegertracing/jaeger/issues/6334
			IncludeSystemCACertsPool: o.Enabled && (o.CAPath == ""),
		},
	}
}

// ToOtelServerConfig provides a mapping between from Options to OTEL's TLS Server Configuration.
func (o *options) ToOtelServerConfig() configoptional.Optional[configtls.ServerConfig] {
	if !o.Enabled {
		return configoptional.None[configtls.ServerConfig]()
	}

	cfg := configtls.ServerConfig{
		ClientCAFile: o.ClientCAPath,
		Config: configtls.Config{
			CAFile:         o.CAPath,
			CertFile:       o.CertPath,
			KeyFile:        o.KeyPath,
			CipherSuites:   o.CipherSuites,
			MinVersion:     o.MinVersion,
			MaxVersion:     o.MaxVersion,
			ReloadInterval: o.ReloadInterval,
		},
	}

	if o.ReloadInterval > 0 {
		cfg.ReloadClientCAFile = true
	}

	return configoptional.Some(cfg)
}
