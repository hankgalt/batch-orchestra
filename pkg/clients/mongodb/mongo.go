package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoStore struct {
	client *mongo.Client
	store  *mongo.Database
}

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

func NewMongoStore(ctx context.Context, cfg MongoConfig) (*MongoStore, error) {
	builder := NewMongoConnectionBuilder(
		cfg.Protocol,
		cfg.Host,
	).WithUser(
		cfg.User,
	).WithPassword(
		cfg.Pwd,
	).WithConnectionParams(
		cfg.Params,
	)
	opts := &MongoStoreOption{
		DBName:   cfg.DBName, // Database name is required for MongoDB operations
		PoolSize: 10,         // Default pool size, can be adjusted based on application needs
	}

	if opts.DBName == "" {
		return nil, fmt.Errorf("missing database name")
	}

	dbConnStr, err := builder.Build()
	if err != nil {
		return nil, err
	}

	mOpts := options.Client().ApplyURI(
		dbConnStr,
	).SetReadPreference(
		readpref.Primary(),
	).SetMaxPoolSize(
		opts.PoolSize,
	)

	cl, err := mongo.Connect(ctx, mOpts)
	if err != nil {
		return nil, err
	}

	if err = cl.Ping(ctx, nil); err != nil {
		if disconnectErr := cl.Disconnect(ctx); disconnectErr != nil {
			return nil, errors.Join(err, disconnectErr)
		}
		return nil, err
	}

	return &MongoStore{
		client: cl,
		store:  cl.Database(opts.DBName),
	}, nil
}

func (ms *MongoStore) AddCollectionDoc(
	ctx context.Context,
	collectionName string,
	doc map[string]any,
) (string, error) {
	if collectionName == "" || doc == nil {
		return "", fmt.Errorf("collection name and document cannot be empty")
	}

	now := time.Now().UTC()
	doc["created_at"] = now
	doc["updated_at"] = now

	coll := ms.store.Collection(collectionName)
	res, err := coll.InsertOne(ctx, doc)
	if err != nil {
		return "", fmt.Errorf("error inserting document into collection %s: %w", collectionName, err)
	}

	id, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		return "", fmt.Errorf("error decoding inserted ID from collection %s", collectionName)
	}
	return id.Hex(), nil
}

func (ms *MongoStore) Close(ctx context.Context) error {
	if err := ms.client.Disconnect(ctx); err != nil && err != mongo.ErrClientDisconnected {
		return err
	}
	return nil
}
