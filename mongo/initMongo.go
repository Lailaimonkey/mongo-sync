package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

//所有集合（自定义查询）
type MonkeyCollections struct {
	user        *MonkeyCollection
	userAccount *MonkeyCollection
}

type MonkeyCollection struct {
	collection *mongo.Collection
}

func InitMasterDatabase() *MonkeyCollections {
	//初始化数据库
	db := initMasterDBClient()

	//初始化集合
	return initMasterCollection(db)
}

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

func initLiveDBClient() *mongo.Database {
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

func initMasterCollection(db *mongo.Database) *MonkeyCollections {
	user := &MonkeyCollection{
		collection: db.Collection("集合名称"),
	}

	userAccount := &MonkeyCollection{
		collection: db.Collection("集合名称"),
	}

	return &MonkeyCollections{
		user:        user,
		userAccount: userAccount,
	}
}