package realtime

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/viant/scy"
	"google.golang.org/api/option"
	"net/url"
	"sync"
)

const (
	credentialsJSON = "credJSON"
	credentialsURL  = "credURL"
	credentialsKey  = "credKey"
	credID          = "credID"
	endpoint        = "endpoint"
	userAgent       = "ua"
	apiKey          = "apiKey"
	quotaProject    = "quotaProject"
	scopes          = "scopes"
	app             = "app"
	defaultApp      = "go-sql-bq"
)

// Config is a configuration parsed from a DSN string.
// If a new Config is created instead of being parsed from a DSN string,
// the NewConfig function should be used, which sets default values.
type Config struct {
	CredentialsFile string // Username
	Endpoint        string
	APIKey          string
	CredentialJSON  []byte
	CredentialsURL  string
	CredID          string //scy secret resource ID
	CredentialsKey  string
	UserAgent       string
	ProjectID       string // project ID
	QuotaProject    string
	Scopes          []string
	Location        string
	DatabaseURL     string
	App             string
	url.Values
}

// hasCredentials returns ture if config has credential configured
func (c *Config) hasCredentials() bool {
	return c.CredID != "" || len(c.CredentialJSON) > 0 || c.CredentialsURL != "" || c.CredentialsFile != ""
}

func (c *Config) options() []option.ClientOption {
	var result = make([]option.ClientOption, 0)
	if c.CredentialsFile != "" {
		result = append(result, option.WithCredentialsFile(c.CredentialsFile))
	}
	if c.Endpoint != "" {
		result = append(result, option.WithEndpoint(c.Endpoint))
	}
	if c.UserAgent != "" {
		result = append(result, option.WithEndpoint(c.UserAgent))
	}
	if c.APIKey != "" {
		result = append(result, option.WithAPIKey(c.APIKey))
	}
	if c.QuotaProject != "" {
		result = append(result, option.WithQuotaProject(c.QuotaProject))
	}
	if len(c.CredentialJSON) > 0 {
		result = append(result, option.WithCredentialsJSON(c.CredentialJSON))
	}

	if len(c.Scopes) > 0 {
		result = append(result, option.WithScopes(c.Scopes...))
	}
	return result
}

// NewConfig creates a new Config and sets default values.
func NewConfig() *Config {
	return &Config{}
}

func (c *Config) initialiseSecrets() error {
	if c.CredentialsURL != "" {
		if URL, err := base64.RawURLEncoding.DecodeString(c.CredentialsURL); err == nil {
			c.CredentialsURL = string(URL)
		}
	}
	if c.CredentialsKey != "" {
		if URL, err := base64.RawURLEncoding.DecodeString(c.CredentialsKey); err == nil {
			c.CredentialsKey = string(URL)
		}
	}
	if len(c.CredentialJSON) > 0 {
		if raw, err := base64.RawURLEncoding.DecodeString(string(c.CredentialJSON)); err == nil {
			c.CredentialJSON = raw
		}
	}
	if c.CredID != "" {
		resource := scy.Resources().Lookup(c.CredID)
		if resource == nil {
			return fmt.Errorf("failed to lookup secretID: %v", c.CredID)
		}
		credentialJSON, err := credentials.lookup(resource)
		if err != nil {
			return err
		}
		c.CredentialJSON = []byte(credentialJSON)
	}

	if c.CredentialsURL != "" {
		credentialJSON, err := credentials.lookup(&scy.Resource{URL: c.CredentialsURL, Key: c.CredentialsKey})
		if err != nil {
			return err
		}
		c.CredentialJSON = []byte(credentialJSON)

	}
	return nil
}

type credentialsRegistry struct {
	registry map[string]string
	sync.RWMutex
	service *scy.Service
}

func (r *credentialsRegistry) lookup(resource *scy.Resource) (string, error) {
	r.RWMutex.RLock()
	result, ok := r.registry[resource.URL]
	r.RWMutex.RUnlock()
	if ok {
		return result, nil
	}

	secrets, err := r.service.Load(context.Background(), resource)
	if err != nil {
		return "", fmt.Errorf("failed to load secret from :%v, %w", resource.URL, err)
	}
	r.RWMutex.Lock()
	r.registry[resource.URL] = secrets.String()
	r.RWMutex.Unlock()
	return secrets.String(), nil
}

var credentials = credentialsRegistry{registry: map[string]string{}, service: scy.New()}
