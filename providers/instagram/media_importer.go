package instagram

import (
	"../../mqtt_helpers"
	"../../neo_helpers"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/cihangir/neo4j"
	"github.com/fatih/structs"
	"github.com/jrallison/go-workers"
	"github.com/maggit/go-instagram/instagram"
	"os"
	"strconv"
)

// MediaImportWorker Imports Instagram media to Neo4J
func MediaImportWorker(message *workers.Msg) {
	igUID, igUIDErr := message.Args().GetIndex(0).String()
	igToken, igTokenErr := message.Args().GetIndex(1).String()

	maxID, maxIDErr := message.Args().GetIndex(2).String()

	if igUIDErr != nil {
		log.Error("MediaImportWorker: Missing IG User ID")
	}

	if igTokenErr != nil {
		log.Error("MediaImportWorker: Mssing IG Token")
		return
	}
	log.Info("MediaImportWorker: Starting NeoMedia Import process: ", igUID)

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	opt := &instagram.Parameters{Count: 20}

	if (maxIDErr == nil) && (maxID != "") {
		opt.MaxID = maxID
	}

	media, next, err := client.Users.RecentMedia(igUID, opt)

	if next != nil {
		//log.Info("MediaImportWorker: We have next Page from IG")
	}

	if err != nil {
		log.Error("InstagramMediaImportWorker: Instagram API Failed : %v", err)
		performMediaAgain(igUID, igToken, maxID)
		return
	}

	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)
	batch := neo4jConnection.NewBatch()

	// Create Media Nodes
	for _, m := range media {
		node := &neo4j.Node{}
		igMediaItem := MediaItem{}
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

		node.Data = structs.Map(igMediaItem)

		mediaItemUnique := &neo4j.Unique{}
		mediaItemUnique.IndexName = "igmedia"
		mediaItemUnique.Key = "InstagramID"
		mediaItemUnique.Value = igMediaItem.InstagramID

		batch.CreateUnique(node, mediaItemUnique)
		batch.Create(neohelpers.CreateCypherLabelOperation(mediaItemUnique, ":InstagramMediaItem"))

		userUnique := &neo4j.Unique{}
		userUnique.IndexName = "igpeople"
		userUnique.Key = "InstagramID"
		userUnique.Value = igUID
		batch.Create(neohelpers.CreateCypherRelationshipOperationFromDifferentIndex(userUnique, mediaItemUnique, "instagram_media_item"))

		if hasVenue {
			// Query Neo4J w/ VENUE IG ID
			query := fmt.Sprintf("match (c:InstagramLocation) where c.InstagramID = '%v' return id(c)", m.Location.ID)
			_, err := neohelpers.FindIDByCypher(neo4jConnection, query)

			mediaLocationIDStr := strconv.Itoa(m.Location.ID)

			if err != nil {
				venueNode := &neo4j.Node{}
				igMediaLocation := MediaLocation{}
				igMediaLocation.Name = m.Location.Name
				igMediaLocation.Latitude = m.Location.Latitude
				igMediaLocation.Longitude = m.Location.Longitude
				igMediaLocation.InstagramID = mediaLocationIDStr

				venueNode.Data = structs.Map(igMediaLocation)

				mediaLocationUnique := &neo4j.Unique{}
				mediaLocationUnique.IndexName = "igmedialocation"
				mediaLocationUnique.Key = "InstagramID"
				mediaLocationUnique.Value = igMediaLocation.InstagramID

				batch.CreateUnique(venueNode, mediaLocationUnique)
				batch.Create(neohelpers.CreateCypherLabelOperation(mediaLocationUnique, ":InstagramLocation"))
				batch.Create(neohelpers.CreateCypherRelationshipOperationFromDifferentIndex(mediaItemUnique, mediaLocationUnique, "instagram_location"))

			} else {
				// Add relationship between NEO VENUE and NEO MEDIA
				mediaLocationUnique := &neo4j.Unique{}
				mediaLocationUnique.IndexName = "igmedialocation"
				mediaLocationUnique.Key = "InstagramID"
				mediaLocationUnique.Value = mediaLocationIDStr
				batch.Create(neohelpers.CreateCypherRelationshipOperationFromDifferentIndex(mediaItemUnique, mediaLocationUnique, "instagram_location"))
			}
		}
	}

	res, err := batch.Execute()
	if err != nil {
		log.Error("MediaImportWorker: THERE WAS AN ERROR EXECUTING BATCH!!!!")
		log.Error(err)
		log.Error(res)
		log.Error("MediaImportWorker: FAILED WITH ARGS| igUID: %v  igToken: %v maxID: %v ", igUID, igToken, maxID)
		performMediaAgain(igUID, igToken, maxID)
	} else {
		log.Info("MediaImportWorker: Successfully imported Media to Neo4J")

		if next.NextMaxID != "" {
			workers.Enqueue("instagramediaimportworker", "MediaImportWorker", []string{igUID, igToken, next.NextMaxID})
		} else {
			log.Info("MediaImportWorker: Done Importing Media for IG User!")

			//Mark this user data as finished importing
			updateQuery := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' SET c.MediaDataImportFinished=true RETURN c.Username", igUID)
			response, updateUserError := neohelpers.UpdateNodeWithCypher(neo4jConnection, updateQuery)
			if updateUserError == nil {
				// Notify Mosquitto MQTT Broker of Media Import Completion
				updatedUserFirstSlice, ok := response[0].([]interface{})
				updatedUserUsername, ok := updatedUserFirstSlice[0].(string)
				if ok {
					mqttURI := os.Getenv("MQTTURI")
					mqtthelpers.PublishMessage(mqttURI, updatedUserUsername, "MediaDataImportFinished")
				}
			} else {
				log.Error("UserimportWorker: error updating user MediaDataImportFinished :(", updateUserError)
			}
		}
	}

}

// Retry due to IG API Rate Limit or another Error
func performMediaAgain(igUID string, igToken string, cursorString string) {
	log.Error("InstagramMediaImportWorker: Retrying Due To Error IGUID: %v", igUID)
	log.Error("InstagramMediaImportWorker: Retrying Due To Error IGTOKEN: %v", igToken)
	if cursorString != "" {
		workers.EnqueueIn("instagramediaimportworker", "MediaImportWorker", 3600.0, []string{igUID, igToken, cursorString, ""})
	} else {
		workers.EnqueueIn("instagramediaimportworker", "MediaImportWorker", 3600.0, []string{igUID, igToken, "", ""})
	}
}
