package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

func initMasterDBClient() *mongo.Database {
	var err error
	clientOptions := options.Client().ApplyURI("mongodb://47.94.142.208:27017/?connect=direct").SetConnectTimeout(5 * time.Second)

	// 连接到MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	//选择数据库
	return client.Database("sc2")
}

func initLiveDBClient() *mongo.Database {
	var err error
	clientOptions := options.Client().ApplyURI("mongodb://47.94.142.208:27017/?connect=direct").SetConnectTimeout(5 * time.Second)

	// 连接到MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	//选择数据库
	return client.Database("sc10")
}

func initSlaveDBClient() *mongo.Database {
	var err error
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017/?connect=direct").SetConnectTimeout(5 * time.Second)

	// 连接到MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	//选择数据库
	return client.Database("sc2")
}
