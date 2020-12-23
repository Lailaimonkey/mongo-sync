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
	clientOptions := options.Client().ApplyURI("mongodb://ip:端口/?connect=direct").SetConnectTimeout(5 * time.Second)

	// 连接到MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	//选择数据库
	return client.Database("数据库")
}

func initSlaveDBClient() *mongo.Database {
	var err error
	clientOptions := options.Client().ApplyURI("mongodb://ip:端口/?connect=direct").SetConnectTimeout(5 * time.Second)

	// 连接到MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	//选择数据库
	return client.Database("数据库")
}
