package main

import (
	"moneky-data-sync/mongo"
)

func main() {

	mongo.Find()

	mongo.Sync()
}
