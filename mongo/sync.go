package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

type StreamObject struct {
	Id                *WatchId `bson:"_id"`
	OperationType     string
	FullDocument      map[string]interface{}
	Ns                NS
	UpdateDescription map[string]interface{}
	DocumentKey       map[string]interface{}
}

type NS struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

type WatchId struct {
	Data string `bson:"_data"`
}

const (
	OperationTypeInsert  = "insert"
	OperationTypeDelete  = "delete"
	OperationTypeUpdate  = "update"
	OperationTypeReplace = "replace"
)

var resumeToken bson.Raw

func Sync() {
	go syncMaster()

	for {
		time.Sleep(2 * time.Second)
	}
}

func syncMaster() {
	for {
		//获得主库数据连接
		client := initMasterDBClient()
		watch(client)
	}
}

func watch(client *mongo.Database) {
	defer func() {
		err := recover()
		if err != nil {
			log.Printf("同步出现异常: %+v \n", err)
		}
	}()

	//设置过滤条件
	pipeline := mongo.Pipeline{
		bson.D{{"$match",
			bson.M{"operationType": bson.M{"$in": bson.A{"insert", "delete", "replace", "update"}}},
		}},
	}

	//当前时间前一小时
	now := time.Now()
	m, _ := time.ParseDuration("-1h")
	now = now.Add(m)
	timestamp := &primitive.Timestamp{
		T: uint32(now.Unix()),
		I: 0,
	}

	//设置监听option
	opt := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetStartAtOperationTime(timestamp)
	if resumeToken != nil {
		opt.SetResumeAfter(resumeToken)
		opt.SetStartAtOperationTime(nil)
	}

	//获得watch监听
	watch, err := client.Watch(context.TODO(), pipeline, opt)
	if err != nil {
		log.Fatal("watch监听失败：", err)
	}

	//获得从库连接
	slaveClient := initSlaveDBClient()

	for watch.Next(context.TODO()) {
		var stream StreamObject
		err = watch.Decode(&stream)
		if err != nil {
			log.Println("watch数据失败：", err)
		}

		log.Println("=============", stream.FullDocument["_id"])

		//保存现在resumeToken
		resumeToken = watch.ResumeToken()

		switch stream.OperationType {
		case OperationTypeInsert:
			syncInsert(slaveClient, stream)
		case OperationTypeDelete:
			filter := bson.M{"_id": stream.FullDocument["_id"]}
			_, err := slaveClient.Collection(stream.Ns.Collection).DeleteOne(context.TODO(), filter)
			if err != nil {
				log.Println("删除失败：", err)
			}
		case OperationTypeUpdate:
			filter := bson.M{"_id": stream.FullDocument["_id"]}
			update := bson.M{"$set": stream.FullDocument}
			_, err := slaveClient.Collection(stream.Ns.Collection).UpdateOne(context.TODO(), filter, update)
			if err != nil {
				log.Println("更新失败：", err)
			}
		case OperationTypeReplace:
			filter := bson.M{"_id": stream.FullDocument["_id"]}
			_, err := slaveClient.Collection(stream.Ns.Collection).ReplaceOne(context.TODO(), filter, stream.FullDocument)
			if err != nil {
				log.Println("替换失败：", err)
			}
		}
	}
}

func syncInsert(slaveClient *mongo.Database, stream StreamObject) {
	defer func() {
		_ = recover()
	}()

	_, err := slaveClient.Collection(stream.Ns.Collection).InsertOne(context.TODO(), stream.FullDocument)
	if err != nil {
		log.Println("插入失败：", err)
	}
}
