package main

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/carbocation/go-instagram/instagram"
	"github.com/hamin/neo4j"
	// "github.com/jinzhu/gorm"
	"github.com/jrallison/go-workers"
	"os"
	"strconv"
	"time"
)

type instagramVenueImport struct {
	LocationID     int
	ExistsInDB     bool
	NeoVenueNodeID int32
	NodeIdx        int
}

// InstagramMediaImportWorker Imports Instagram media to Neo4J
func InstagramMediaImportWorker(message *workers.Msg) {

	igUID, igUIDErr := message.Args().GetIndex(0).String()
	log.Info("Starting NeoMedia Import process: ", igUID)

	igToken, igTokenErr := message.Args().GetIndex(1).String()
	log.Info("Starting NeoMedia Import process: ", igToken)

	maxID, maxIDErr := message.Args().GetIndex(2).String()

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	opt := &instagram.Parameters{Count: 20}

	if (maxIDErr == nil) && (maxID != "") {
		log.Info("We have a Max ID")
		opt.MaxID = maxID
	}

	media, next, err := client.Users.RecentMedia(igAuth.UID, opt)
	if err != nil {
		log.Error("Error: %v\n", err)
		if (err == nil) && (maxID != "") {
			log.Info("Instagram API Failed, Enqueuing Again with MaxID: ", maxID)
			workers.Enqueue("neomediaimporter", "NeoMediaImportWorker", []string{igUID, igToken, maxID})
		} else {
			log.Info("Instagram API Failed, Enqueuing Again")
			workers.Enqueue("neomediaimporter", "NeoMediaImportWorker", []string{igUID, igToken})
		}
		return
	}

}
