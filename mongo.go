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
	dataqueue  chan bson.M
	stopflag   bool
}

var Client *MongoClient
var Ctx context.Context

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
	Ctx, _ = context.WithTimeout(context.Background(), 6*time.Second)
	err = client.Connect(context.Background())
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
	return &MongoCollection{collection: db.database.Collection(collection), dataqueue: make(chan bson.M, 1000), stopflag: false}
}
func (mc *MongoClient) Close() error {
	er := mc.client.Disconnect(Ctx)
	if er == nil {
		mc.client = nil
	}
	return er
}

func (c *MongoCollection) create(item bson.M) bool {
	_, crerr := c.collection.InsertOne(context.Background(), item)
	return crerr == nil
}

func (c *MongoCollection) updateOne(id bson.D, fields bson.M) bool {
	_, uerr := c.collection.UpdateOne(context.Background(), id, fields)
	return uerr == nil
}
func (c *MongoCollection) exist(id bson.D) bool {
	_, res := c.collection.FindOne(context.Background(), id).DecodeBytes()
	return res == nil
}

//fields format: {"_id":1,"data":{"_id":1,"open":12.7..}, push_key:"data"}
func (c *MongoCollection) Push(fields bson.M) {
	c.dataqueue <- fields
}

func (c *MongoCollection) GetValueToString(fields bson.M, key string) (string, bool) {
	if data, ok := fields[key]; ok {
		return data.(string), true
	} else {
		return "", false
	}
}
func (c *MongoCollection) GetMapValue(fields bson.M, key string) (map[string]interface{}, bool) {
	if data, ok := fields[key]; ok {
		return data.(map[string]interface{}), true
	} else {
		return nil, false
	}
}

func (c *MongoCollection) HandleLoop() {

	for {
		if c.stopflag {
			break
		}
		select {
		case data := <-c.dataqueue:
			{
				fmt.Println(data)
				if sid, ok := data["_id"]; ok {
					id := sid.(int64)
					key, ok := c.GetValueToString(data, "push_key")
					if !ok {
						break
					}
					interdata, ok := c.GetMapValue(data, key)
					if !ok {
						break
					}
					if c.exist(bson.D{{"_id", id}}) {
						updatedata := bson.M{"$push": bson.M{key: interdata}}
						fmt.Println("update", updatedata)
						c.updateOne(bson.D{{"_id", id}}, updatedata)

					} else {
						createdata := bson.M{"_id": id, key: []bson.M{interdata}}
						fmt.Println("create", createdata)
						c.create(createdata)
					}

				}

			}
		}
	}

}
