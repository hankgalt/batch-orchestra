package mongodb

import (
	"fmt"
	"strings"
)

type MongoConfig struct {
	Protocol string // e.g., "mongodb", "mongodb+srv"
	Host     string
	Port     int
	User     string
	Pwd      string
	Params   string
	DBName   string
}

type MongoStoreOption struct {
	DBName   string
	PoolSize uint64
}

// MongoConnectionBuilder is a ConnectionBuilder implementation for creating MongoDB connection strings.
type MongoConnectionBuilder struct {
	protocol string
	host     string
	user     string
	pwd      string
	params   string
}

// NewMongoConnectionBuilder creates a new mongoConnectionBuilder
// with the specified protocol and host & returns instance pointer.
func NewMongoConnectionBuilder(p, h string) MongoConnectionBuilder {
	return MongoConnectionBuilder{
		protocol: p,
		host:     h,
	}
}

func (b MongoConnectionBuilder) WithUser(u string) MongoConnectionBuilder {
	b.user = u
	return b
}

func (b MongoConnectionBuilder) WithPassword(p string) MongoConnectionBuilder {
	b.pwd = p
	return b
}

func (b MongoConnectionBuilder) WithConnectionParams(p string) MongoConnectionBuilder {
	b.params = p
	return b
}

// Build constructs the MongoDB connection string based on the provided parameters.
// It returns an error if required parameters are missing or if the protocol is "mongodb" and
// connection parameters are not provided.
// The connection string is formatted as "[protocol]://[user[:password]@]host[:port][/params]".
func (b MongoConnectionBuilder) Build() (string, error) {
	if b.protocol == "" || b.host == "" {
		return "", fmt.Errorf("missing required parameters: protocol and host are required")
	}

	if b.protocol == "mongodb" && b.params == "" {
		return "", fmt.Errorf("missing required connection parameters")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s://", b.protocol))

	if b.user != "" {
		sb.WriteString(b.user)
		if b.pwd != "" {
			sb.WriteString(":" + b.pwd)
		}
		sb.WriteString("@")
	}

	sb.WriteString(b.host)

	if b.protocol == "mongodb" {
		sb.WriteString("/" + b.params)
	}

	return sb.String(), nil
}
