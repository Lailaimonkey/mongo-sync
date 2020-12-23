package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/mongo/driver/description"
	"log"
)

type Collection struct {
	name           string
	readConcern    *readconcern.ReadConcern
	writeConcern   *writeconcern.WriteConcern
	readPreference *readpref.ReadPref
	readSelector   description.ServerSelector
	writeSelector  description.ServerSelector
	registry       *bsoncodec.Registry
}

func (coll *MonkeyCollection) FindOne(ctx context.Context, filter interface{},
	results interface{}, opts ...*options.FindOptions) {

	err := coll.collection.FindOne(ctx, filter).Decode(results)
	if err != nil {
		log.Fatal("查询数据失败：", err)
	}
}

func (coll *MonkeyCollection) Find(ctx context.Context, filter interface{},
	results interface{}, opts ...*options.FindOptions) {

	find, err := coll.collection.Find(ctx, filter)
	if err != nil {
		log.Fatal("查询数据失败：", err)
	}
	find.All(context.TODO(), results)
}

func (coll *MonkeyCollection) UpdateOne(ctx context.Context, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) {

	_, err := coll.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		log.Fatal("更新数据失败：", err)
	}
}

func (coll *MonkeyCollection) UpdateMany(ctx context.Context, filter interface{}, update interface{},
	opts ...*options.UpdateOptions) {
	_, err := coll.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		log.Fatal("更新数据失败：", err)
	}
}

func (coll *MonkeyCollection) InsertOne(ctx context.Context, document interface{},
	opts ...*options.InsertOneOptions) interface{} {

	one, err := coll.collection.InsertOne(ctx, document)
	if err != nil {
		log.Fatal("插入数据失败：", err)
	}
	return one.InsertedID
}

func (coll *MonkeyCollection) InsertMany(ctx context.Context, documents []interface{},
	opts ...*options.InsertManyOptions) []interface{} {

	many, err := coll.collection.InsertMany(ctx, documents)
	if err != nil {
		log.Fatal("插入数据失败：", err)
	}
	return many.InsertedIDs
}
