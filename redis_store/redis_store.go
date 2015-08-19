package redisstore

import (
	"github.com/albrow/zoom"
	"log"
)

// RedisStorePool - Accessible Redis Store Pool
var RedisStorePool *zoom.Pool

// FollowerRecord - zoom model type
var FollowerRecord *zoom.ModelType

// NewFollowerRecord - new follower record schema
type NewFollowerRecord struct {
	OriginalUserIGUID     string `zoom:"index"`
	FollowerIGUID         string `zoom:"index"`
	MediaImportFinished   bool
	FollowsImportFinished bool
	zoom.RandomId
}

// InitRedisStore - Initialize the Redis Store Pool
func InitRedisStore(poolConfig *zoom.PoolConfig) {
	RedisStorePool = zoom.NewPool(poolConfig)

	var err error
	FollowerRecord, err = RedisStorePool.Register(&NewFollowerRecord{})
	if err != nil {
		log.Fatal(err)
	}
}

// ClosePool - Closes redis store pool
func ClosePool() error {
	return RedisStorePool.Close()
}
