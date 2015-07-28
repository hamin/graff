package main

import (
	// "errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/carbocation/go-instagram/instagram"
	"github.com/cihangir/neo4j"
	"github.com/fatih/structs"
	"github.com/jrallison/go-workers"
	"os"
	"strconv"
)

type instagramVenueImport struct {
	LocationID     int
	ExistsInDB     bool
	NeoVenueNodeID int32
	NodeIdx        int
}

// InstagramMediaItem - This is media object from IG
type InstagramMediaItem struct {
	Type                string
	Filter              string
	Link                string
	UserHasLiked        bool
	CreatedTime         int64
	InstagramID         string
	ImageThumbnail      string
	ImageLowResolution  string
	ImageHighResolution string
	VideoLowResolution  string
	VideoHighResolution string
	CaptionID           string
	CaptionText         string
	LikesCount          int
	HasVenue            bool
}

// InstagramMediaLocation - This is media object from IG
type InstagramMediaLocation struct {
	InstagramID string
	Name        string
	Latitude    float64
	Longitude   float64
}

// InstagramMediaImportWorker Imports Instagram media to Neo4J
func InstagramMediaImportWorker(message *workers.Msg) {

	igUID, igUIDErr := message.Args().GetIndex(0).String()
	log.Info("Starting NeoMedia Import process: ", igUID)

	igToken, igTokenErr := message.Args().GetIndex(1).String()
	log.Info("Starting NeoMedia Import process: ", igToken)

	maxID, maxIDErr := message.Args().GetIndex(2).String()

	userNeoNodeID, userNeoNodeIDErr := message.Args().GetIndex(3).Int()

	if igUIDErr != nil {
		log.Error("Missing IG User ID")
	}

	if igTokenErr != nil {
		log.Error("Mssing IG Token")
	}

	if userNeoNodeIDErr != nil {
		log.Error("Missing IG User Neo Node ID")
	}

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	opt := &instagram.Parameters{Count: 20}

	if (maxIDErr == nil) && (maxID != "") {
		log.Info("We have a Max ID")
		opt.MaxID = maxID
	}

	media, next, err := client.Users.RecentMedia(igUID, opt)

	if next != nil {
		log.Info("We have next Page from IG")
	}

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

	var nodeIdx int
	nodeIdx = 0

	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)
	batch := neo4jConnection.NewBatch()

	batchOperations := []*neo4j.ManuelBatchRequest{}
	importedIGVenues := make(map[int]instagramVenueImport)

	for _, m := range media {
		var mediaItemNodeIdx = nodeIdx
		node := &neo4j.Node{}
		igMediaItem := InstagramMediaItem{}
		igMediaItem.InstagramID = m.ID
		igMediaItem.Filter = m.Filter
		igMediaItem.Link = m.Link
		igMediaItem.LikesCount = m.Likes.Count
		igMediaItem.ImageThumbnail = m.Images.Thumbnail.URL
		igMediaItem.CreatedTime = m.CreatedTime

		if m.Images != nil {
			igMediaItem.ImageLowResolution = m.Images.LowResolution.URL
			igMediaItem.ImageHighResolution = m.Images.StandardResolution.URL
		}

		if m.Videos != nil {
			igMediaItem.VideoLowResolution = m.Videos.LowResolution.URL
			igMediaItem.VideoLowResolution = m.Videos.StandardResolution.URL
		}

		if m.Caption != nil {
			igMediaItem.CaptionText = m.Caption.Text
			igMediaItem.CaptionID = m.Caption.ID
		}

		hasVenue := false
		if m.Location != nil {
			var location = m.Location
			if location.Name != "" {
				hasVenue = true
			}
		}
		igMediaItem.HasVenue = hasVenue

		// node.Data = data
		node.Data = structs.Map(igMediaItem)
		batch.Create(node)
		//ADD LABEL FOR MEDIA NODE WITH {INDEX} = nodeIdx
		AddLabelOperation(&batchOperations, nodeIdx, "InstagramMediaItem")

		// TODO NEED TO ADD RELATIONSHIP TO IG USER NEO NODE
		AddRelationshipOperation(&batchOperations, int(userNeoNodeID), mediaItemNodeIdx, true, false, "neo_media_item")

		// Handle has_venue is true
		if hasVenue {
			// First check to see if we've already imported this Venue in this range loop
			// this migth or might not exist in a batch operation
			existingVenueImport, ok := importedIGVenues[m.Location.ID]
			if ok {
				// We've already done a venue import for this venue
				if existingVenueImport.ExistsInDB {
					AddRelationshipOperation(&batchOperations, mediaItemNodeIdx, int(existingVenueImport.NeoVenueNodeID), false, true, "neo_venue")
				} else {
					AddRelationshipOperation(&batchOperations, mediaItemNodeIdx, existingVenueImport.NodeIdx, false, false, "neo_venue")
				}
			} else {

				newVenueImport := instagramVenueImport{LocationID: m.Location.ID}

				// Query Neo4J w/ VENUE IG ID
				query := fmt.Sprintf("match (c:NeoVenue) where c.instagram_id = '%v' return id(c)", m.Location.ID)
				log.Info("THIS IS THE IGVENUE INSTAGRAM ID: %v", m.Location.ID)
				log.Info("THIS IS CYPHER QUERY: %v", query)
				exstingLocationNeoNodeID, err := FindByCypher(neo4jConnection, query)
				log.Info("THIS IS THE NODEID FOR THE VENUE: %v", exstingLocationNeoNodeID)

				if err != nil {
					log.Info("Error trying to find NeoVenue w/ InstagramID: %v", m.Location.ID)
					venueNode := &neo4j.Node{}
					igMediaLocation := InstagramMediaLocation{}
					igMediaLocation.Name = m.Location.Name
					igMediaLocation.Latitude = m.Location.Latitude
					igMediaLocation.Longitude = m.Location.Longitude
					igMediaLocation.InstagramID = strconv.Itoa(m.Location.ID)

					venueNode.Data = structs.Map(igMediaLocation)
					batch.Create(venueNode)
					nodeIdx++

					newVenueImport.ExistsInDB = false
					newVenueImport.NodeIdx = nodeIdx
					importedIGVenues[newVenueImport.LocationID] = newVenueImport

					// Add Label for NEO Venue
					AddLabelOperation(&batchOperations, nodeIdx, "NeoVenue")
					// Add relationship between NEO VENUE and NEO MEDIA
					AddRelationshipOperation(&batchOperations, mediaItemNodeIdx, nodeIdx, false, false, "neo_venue")
				} else {
					log.Info("THERE IS AN EXISTING NEO VENUE NODE SO WE WILL")
					newVenueImport.ExistsInDB = true
					newVenueImport.NeoVenueNodeID = int32(exstingLocationNeoNodeID)
					importedIGVenues[newVenueImport.LocationID] = newVenueImport

					// Add relationship between NEO VENUE and NEO MEDIA
					AddRelationshipOperation(&batchOperations, mediaItemNodeIdx, exstingLocationNeoNodeID, false, true, "neo_venue")
				}
			}

		}

		nodeIdx++
	}

	for _, batchOp := range batchOperations {
		batch.Create(batchOp)
	}

	res, err := batch.Execute()
	if err != nil {
		log.Error("THERE WAS AN ERROR EXECUTING BATCH!!!!")
		log.Error(err)
		log.Error(res)
	} else {
		log.Info("Successfully imported Media to Neo4J")

		if next.NextMaxID != "" {
			log.Info("*** This is our next.NextMaxID ", next.NextMaxID)
			workers.Enqueue("instagramediaimportworker", "InstagramMediaImportWorker", []string{igUID, igToken, next.NextMaxID, string(userNeoNodeID)})
			log.Info("Enqueued Next Pagination Media Import!!!")
		} else {
			log.Info("Done Importing Media for IG User!")
		}
	}

}
