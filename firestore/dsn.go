package firestore

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
)

const (
	firestoreScheme = "firestore"
)

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (*Config, error) {
	URL, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid dsn: %v", err)
	}
	if URL.Scheme != firestoreScheme {
		return nil, fmt.Errorf("invalid dsn scheme, expected %v, but had: %v", firestoreScheme, URL.Scheme)
	}

	path := strings.Trim(URL.Path, "/")
	location := ""
	if index := strings.Index(path, "/"); index != -1 {
		location = path[:index]
		path = path[index+1:]
	}
	cfg := &Config{
		ProjectID: URL.Host,
		Location:  location,
		Values:    URL.Query(),
	}
	if len(cfg.Values) > 0 {
		if _, ok := cfg.Values[endpoint]; ok {
			cfg.Endpoint = cfg.Values.Get(endpoint)
		}
		if _, ok := cfg.Values[userAgent]; ok {
			cfg.UserAgent = cfg.Values.Get(userAgent)
		}
		if _, ok := cfg.Values[apiKey]; ok {
			cfg.APIKey = cfg.Values.Get(apiKey)
		}
		if _, ok := cfg.Values[app]; ok {
			cfg.App = cfg.Values.Get(app)
		}
		if _, ok := cfg.Values[credID]; ok {
			cfg.CredID = cfg.Values.Get(credID)
		}
		if _, ok := cfg.Values[credentialsJSON]; ok {
			cfg.CredentialJSON = []byte(cfg.Values.Get(credentialsJSON))
		}
		if _, ok := cfg.Values[credentialsKey]; ok {
			cfg.CredentialsKey = cfg.Values.Get(credentialsKey)
		}
		if _, ok := cfg.Values[credentialsURL]; ok {
			cfg.CredentialsURL = cfg.Values.Get(credentialsURL)
		}
		if _, ok := cfg.Values[quotaProject]; ok {
			cfg.QuotaProject = cfg.Values.Get(quotaProject)
		}
		if _, ok := cfg.Values[scopes]; ok {
			cfg.Scopes = cfg.Values[scopes]
		}
	}

	if cfg.CredentialsKey != "" {
		if URL, err := base64.RawURLEncoding.DecodeString(cfg.CredentialsKey); err == nil {
			cfg.CredentialsKey = string(URL)
		}
	}

	if err = cfg.initialiseSecrets(); err != nil {
		return nil, err
	}

	if cfg.App == "" {
		cfg.App = defaultApp
	}
	if cfg.Location == "" {
		cfg.Location = "us"
	}
	return cfg, nil
}
