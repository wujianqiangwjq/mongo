package mongo

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoClient struct {
	client *mongo.Client
}
type MongoDb struct {
	database *mongo.Database
}

type MongoCollection struct {
	collection *mongo.Collection
}

var Client *MongoClient
var ctx context.Context

func init() {
	flag := true
	var uri string
	host := os.Getenv("MONGO_HOST")
	if host == "" {
		host = "127.0.0.1"

	}
	port := os.Getenv("MONGO_PORT")
	if port == "" {
		port = "27017"
	}
	username := os.Getenv("MONGO_USER")
	password := os.Getenv("MONGO_PASSWORD")
	if username == "" && password == "" {
		flag = false
	}
	if flag {
		if username == "" || password == "" {
			panic("username/passord is invalid")
			return
		}
		uri = fmt.Sprintf("mongodb+srv://%s:%s@%s:%s",
			username, password,
			host, port)
	} else {
		uri = fmt.Sprintf("mongodb://%s:%s", host, port)
	}

	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
		return
	}
	ctx, _ = context.WithTimeout(context.Background(), 6*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		panic(err)
	}
	Client = &MongoClient{client: client}
	// collectinstance = Client.Database("scheduler").Collection("jobs")
}

func (mc *MongoClient) GetDb(db string) *MongoDb {
	return &MongoDb{database: mc.client.Database(db)}
}
func (db *MongoDb) GetCollection(collection string) *MongoCollection {
	return &MongoCollection{collection: db.database.Collection(collection)}
}
func (mc *MongoClient) Close() error {
	er := mc.client.Disconnect(ctx)
	if er == nil {
		mc.client = nil
	}
	return er
}

func (c *MongoCollection) Create(item bson.M) bool {
	_, crerr := c.collection.InsertOne(ctx, item)
	return crerr == nil
}

func (c *MongoCollection) UpdateOne(id bson.D, fields bson.M) bool {
	_, uerr := c.collection.UpdateOne(ctx, id, fields)
	return uerr == nil
}
