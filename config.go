package metrics

import "fmt"

type Config struct {
	APIAddr   string
	APIToken  string
	ClusterID string
	Insecure  bool
}

func (c Config) validate() error {
	if c.APIAddr == "" {
		return fmt.Errorf("api address is required")
	}

	if c.APIToken == "" {
		return fmt.Errorf("api token is required")
	}

	if c.ClusterID == "" {
		return fmt.Errorf("cluster id is required")
	}

	return nil
}
