package main

import (
	"./providers/instagram"
	"./redis_store"
	log "github.com/Sirupsen/logrus"
	"github.com/albrow/zoom"
	"github.com/jrallison/go-workers"
	"os"
	"runtime"
)

func main() {
	if runtime.NumCPU() > 1 {
		numOfCpus := runtime.NumCPU() - 1
		runtime.GOMAXPROCS(numOfCpus)
		log.Info("Num of CPUs running: ", numOfCpus)
	}
	redisServer := os.Getenv("REDIS_SERVER")
	redisDB := os.Getenv("REDIS_DB")
	redisPool := os.Getenv("REDIS_POOL")
	redisPwd := os.Getenv("REDIS_PWD")

	if (redisServer == "") || (redisDB == "") || (redisPool == "") {
		log.Error("Please Start Worker with Required Arguments")
		return
	}

	rediSStoreConfig := zoom.PoolConfig{}
	rediSStoreConfig.Address = redisServer
	rediSStoreConfig.Database = 11
	rediSStoreConfig.Password = redisPwd

	redisstore.InitRedisStore(&rediSStoreConfig)

	defer func() {
		if err := redisstore.ClosePool(); err != nil {
			// handle error
			log.Error("Redis Store Error: %v", err)
			return
		}
	}()

	workers.Configure(map[string]string{
		// location of redis instance
		"namespace": "graff",
		"server":    redisServer,
		// instance of the database
		"database": redisDB,
		// redis password
		"password": redisPwd,
		// number of connections to keep open with redis
		"pool": redisPool,
		// unique process id for this instance of workers (for proper recovery of inprogress jobs on crash)
		"process": "1",
	})

	// pull messages from "myqueue" with concurrency of 10
	workers.Process("instagramediaimportworker", instagram.MediaImportWorker, 90)
	workers.Process("instagramuserimportworker", instagram.UserImportWorker, 10)
	workers.Process("instagramfollowsimportworker", instagram.FollowsImportWorker, 10)
	workers.Process("instagramfollowersimportworker", instagram.FollowersImportWorker, 10)
	workers.Process("instagramsearchimportworker", instagram.SearchImportWorker, 10)

	// stats will be available at http://localhost:8080/stats
	// go workers.StatsServer(5000)

	// Blocks until process is told to exit via unix signal
	workers.Run()
}
