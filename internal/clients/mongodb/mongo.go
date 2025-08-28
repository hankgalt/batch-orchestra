package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	ErrMsgMongoMissingDBName            = "missing database name"
	ErrMsgMongoCollectionNameOrDocEmpty = "collection name and document cannot be empty"
)

var (
	ErrMongoMissingDBName            = errors.New(ErrMsgMongoMissingDBName)
	ErrMongoCollectionNameOrDocEmpty = errors.New(ErrMsgMongoCollectionNameOrDocEmpty)
)

type MongoStore struct {
	client *mongo.Client
	store  *mongo.Database
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
		return nil, ErrMongoMissingDBName
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
		return "", ErrMongoCollectionNameOrDocEmpty
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
