package instagram

import (
	// "errors"
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

type instagramLocationImport struct {
	LocationID     int
	ExistsInNeo4J  bool
	NeoVenueNodeID int32
	NodeIdx        int
}

// MediaImportWorker Imports Instagram media to Neo4J
func MediaImportWorker(message *workers.Msg) {
	igUID, igUIDErr := message.Args().GetIndex(0).String()
	log.Info("MediaImportWorker: Starting NeoMedia Import process: ", igUID)

	igToken, igTokenErr := message.Args().GetIndex(1).String()
	log.Info("MediaImportWorker: Starting NeoMedia Import process: ", igToken)

	maxID, maxIDErr := message.Args().GetIndex(2).String()

	userNeoNodeIDRaw, userNeoNodeIDErr := message.Args().GetIndex(3).String()
	userNeoNodeID, _ := strconv.Atoi(userNeoNodeIDRaw)
	if igUIDErr != nil {
		log.Error("FollowsImportWorker: Missing IG User ID")
	}

	if igUIDErr != nil {
		log.Error("MediaImportWorker: Missing IG User ID")
	}

	if igTokenErr != nil {
		log.Error("MediaImportWorker: Mssing IG Token")
	}

	if userNeoNodeIDErr != nil {
		log.Error("MediaImportWorker: Missing IG User Neo Node ID")
	}

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	opt := &instagram.Parameters{Count: 20}

	if (maxIDErr == nil) && (maxID != "") {
		log.Info("MediaImportWorker: We have a Max ID")
		opt.MaxID = maxID
	}

	media, next, err := client.Users.RecentMedia(igUID, opt)

	if next != nil {
		log.Info("MediaImportWorker: We have next Page from IG")
	}

	if err != nil {
		log.Error("Error: %v\n", err)
		if (err == nil) && (maxID != "") {
			log.Info("MediaImportWorker: Instagram API Failed, Enqueuing Again with MaxID: ", maxID)
			workers.EnqueueIn("instagramediaimportworker", "InstagramMediaImportWorker", 3600.0, []string{igUID, igToken, maxID, string(userNeoNodeID)})
		} else {
			log.Info("MediaImportWorker: Instagram API Failed, Enqueuing Again")
			workers.EnqueueIn("instagramediaimportworker", "InstagramMediaImportWorker", 3600.0, []string{igUID, igToken, "", string(userNeoNodeID)})
		}
		return
	}

	var nodeIdx int
	nodeIdx = 0

	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)
	batch := neo4jConnection.NewBatch()

	batchOperations := []*neo4j.ManuelBatchRequest{}
	importedIGLocations := make(map[int]instagramLocationImport)

	for _, m := range media {
		var mediaItemNodeIdx = nodeIdx
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

		// node.Data = data
		node.Data = structs.Map(igMediaItem)
		batch.Create(node)
		//ADD LABEL FOR MEDIA NODE WITH {INDEX} = nodeIdx
		neohelpers.AddLabelOperation(&batchOperations, nodeIdx, "InstagramMediaItem")

		// TODO NEED TO ADD RELATIONSHIP TO IG USER NEO NODE
		neohelpers.AddRelationshipOperation(&batchOperations, int(userNeoNodeID), mediaItemNodeIdx, true, false, "instagram_media_item")

		// Handle has_venue is true
		if hasVenue {
			// First check to see if we've already imported this Venue in this range loop
			// this migth or might not exist in a batch operation
			existingVenueImport, ok := importedIGLocations[m.Location.ID]
			if ok {
				// We've already done a location import for this location in this worker
				if existingVenueImport.ExistsInNeo4J {
					neohelpers.AddRelationshipOperation(&batchOperations, mediaItemNodeIdx, int(existingVenueImport.NeoVenueNodeID), false, true, "instagram_location")
				} else {
					neohelpers.AddRelationshipOperation(&batchOperations, mediaItemNodeIdx, existingVenueImport.NodeIdx, false, false, "instagram_location")
				}
			} else {

				newVenueImport := instagramLocationImport{LocationID: m.Location.ID}

				// Query Neo4J w/ VENUE IG ID
				query := fmt.Sprintf("match (c:InstagramLocation) where c.InstagramID = '%v' return id(c)", m.Location.ID)
				log.Info("MediaImportWorker: THIS IS THE IGVENUE INSTAGRAM ID: %v", m.Location.ID)
				log.Info("MediaImportWorker: THIS IS CYPHER QUERY: %v", query)
				exstingLocationNeoNodeID, err := neohelpers.FindIDByCypher(neo4jConnection, query)
				log.Info("MediaImportWorker: THIS IS THE NODEID FOR THE VENUE: %v", exstingLocationNeoNodeID)

				if err != nil {
					log.Info("Error trying to find NeoVenue w/ InstagramID: %v", m.Location.ID)
					venueNode := &neo4j.Node{}
					igMediaLocation := MediaLocation{}
					igMediaLocation.Name = m.Location.Name
					igMediaLocation.Latitude = m.Location.Latitude
					igMediaLocation.Longitude = m.Location.Longitude
					igMediaLocation.InstagramID = strconv.Itoa(m.Location.ID)

					venueNode.Data = structs.Map(igMediaLocation)
					batch.Create(venueNode)
					nodeIdx++

					newVenueImport.ExistsInNeo4J = false
					newVenueImport.NodeIdx = nodeIdx
					importedIGLocations[newVenueImport.LocationID] = newVenueImport

					// Add Label for NEO Venue
					neohelpers.AddLabelOperation(&batchOperations, nodeIdx, "InstagramLocation")
					// Add relationship between NEO VENUE and NEO MEDIA
					neohelpers.AddRelationshipOperation(&batchOperations, mediaItemNodeIdx, nodeIdx, false, false, "instagram_location")
				} else {
					log.Info("MediaImportWorker: THERE IS AN EXISTING NEO VENUE NODE SO WE WILL")
					newVenueImport.ExistsInNeo4J = true
					newVenueImport.NeoVenueNodeID = int32(exstingLocationNeoNodeID)
					importedIGLocations[newVenueImport.LocationID] = newVenueImport

					// Add relationship between NEO VENUE and NEO MEDIA
					neohelpers.AddRelationshipOperation(&batchOperations, mediaItemNodeIdx, exstingLocationNeoNodeID, false, true, "instagram_location")
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
		log.Error("MediaImportWorker: THERE WAS AN ERROR EXECUTING BATCH!!!!")
		log.Error(err)
		log.Error(res)
	} else {
		log.Info("MediaImportWorker: Successfully imported Media to Neo4J")

		if next.NextMaxID != "" {
			log.Info("MediaImportWorker *** This is our next.NextMaxID ", next.NextMaxID)
			workers.Enqueue("instagramediaimportworker", "InstagramMediaImportWorker", []string{igUID, igToken, next.NextMaxID, string(userNeoNodeID)})
			log.Info("MediaImportWorker Enqueued Next Pagination Media Import!!!")
		} else {
			log.Info("MediaImportWorker: Done Importing Media for IG User!")
			//We should mark this user data as finished importing
			updateQuery := fmt.Sprintf("match (c:InstagramUser) where id(c)= %v SET c.MediaDataImportFinished=true", userNeoNodeID)
			response, updateUserError := neohelpers.UpdateNodeWithCypher(neo4jConnection, updateQuery)
			if updateUserError == nil {
				log.Info("MediaImportWorker: UPDATING NODE WITH CYPHER successfully MediaDataImportFinished=true", response)
			} else {
				log.Error("UserimportWorker: error updating user MediaDataImportFinished :(", updateUserError)
			}
		}
	}

}
