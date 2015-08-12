package instagram

import (
	"../../neo_helpers"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/cihangir/neo4j"
	"github.com/fatih/structs"
	"github.com/jrallison/go-workers"
	"github.com/maggit/go-instagram/instagram"
	"github.com/mitchellh/mapstructure"
	"os"
	"strconv"
)

// FollowersImportWorker - Imports an Instagram User's Followers to Neo4J
func FollowersImportWorker(message *workers.Msg) {
	igUID, igUIDErr := message.Args().GetIndex(0).String()
	igToken, igTokenErr := message.Args().GetIndex(1).String()

	followersLimitString, followersLimitError := message.Args().GetIndex(2).String()
	followersLimit, _ := strconv.Atoi(followersLimitString)

	cursorString, cursorErr := message.Args().GetIndex(3).String()
	cursor, _ := strconv.Atoi(cursorString)

	if igUIDErr != nil {
		log.Error("FollowersImportWorker: Missing IG User ID")
		return
	}

	if igTokenErr != nil {
		log.Error("FollowersImportWorker: Mssing IG Token")
		return
	}
	log.Info("Starting FollowersImportWorker Import process: ", igUID)

	client := instagram.NewClient(nil)
	client.AccessToken = igToken
	opt := &instagram.Parameters{}

	if (followersLimitError == nil) && (followersLimit > 0) {
		opt.Count = uint64(followersLimit)
	} else {
		opt.Count = 6
	}

	// TODO: Use the cursor string for pagination
	if (cursorErr == nil) && (cursor > 0) {
		opt.Cursor = uint64(cursor)
	}

	users, _, err := client.Relationships.FollowedBy(igUID, opt)
	if err != nil {
		log.Error("FollowersImportWorkerError:", err)
		performFollowersAgain(igUID, igToken)
		return
	}

	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)
	batch := neo4jConnection.NewBatch()

	for _, u := range users {
		log.Info("FollowersImportWorker ID: %v, Username: %v\n", u.ID, u.Username)
		// Query if we already have imported user to Neo
		query := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' return id(c), c.MediaDataImportStarted, c.MediaDataImportFinished", u.ID)
		response, _ := neohelpers.FindUserByCypher(neo4jConnection, query)

		if len(response) > 0 {
			// User Exists just Add Neo Relationship
			userResponse, ok := response[0].([]interface{})
			if userResponse[0] != nil && ok {
				unique := &neo4j.Unique{}
				unique.IndexName = "igpeople"
				unique.Key = "InstagramID"
				unique.Value = u.ID
				batch.Create(neohelpers.CreateCypherRelationshipOperationTo(igUID, unique, "instagram_follows"))
			}
		} else {
			// Create Neo User & then add relationship
			node := &neo4j.Node{}
			igNeoUser := User{}
			igNeoUser.InstagramID = u.ID
			igNeoUser.FullName = u.FullName
			igNeoUser.ProfilePicture = u.ProfilePicture
			igNeoUser.Username = u.Username
			igNeoUser.MediaDataImportStarted = true

			node.Data = structs.Map(igNeoUser)

			unique := &neo4j.Unique{}
			unique.IndexName = "igpeople"
			unique.Key = "InstagramID"
			unique.Value = u.ID

			batch.CreateUnique(node, unique)
			batch.Create(neohelpers.CreateCypherLabelOperation(unique, ":InstagramUser"))
			batch.Create(neohelpers.CreateCypherRelationshipOperationTo(igUID, unique, "instagram_follows"))
		}
	}

	res, err := batch.Execute()
	if err != nil {
		log.Error("FollowersImportWorker: THERE WAS AN ERROR EXECUTING BATCH!!!!")
		log.Error(err)
		performFollowersAgain(igUID, igToken)
	} else {
		log.Info("FollowersImportWorker: Successfully imported Media to Neo4J")

		// For Imported Users In the Batch
		for _, r := range res {
			if r.Body != nil {

				// Check if nodes in Batch Exists in Neo4j
				if r.Body.(map[string]interface{})["columns"] != nil {

					if len(r.Body.(map[string]interface{})["columns"].([]interface{})) > 1 {
						nodeResponseSlice, ok := r.Body.(map[string]interface{})["data"].([]interface{})
						if !ok {
							return
						}

						nodeResponseElement := nodeResponseSlice[0].([]interface{})[1]
						nodeReponse := &neo4j.NodeResponse{}
						nodeReponseErr := mapstructure.Decode(nodeResponseElement, &nodeReponse)
						if nodeReponseErr != nil {
							log.Error("FollowersImportWorker: nodeReponseErr %v", nodeReponseErr)
							return
						}

						// Import Existing User Node's Data if it Hasn't Been imported
						if nodeReponse.Data["MediaDataImportStarted"] != true {
							// Let's make sure to Enquque Media+Follows Importers
							igUserIDString := nodeReponse.Data["InstagramID"].(string)
							workers.Enqueue("instagramfollowsimportworker", "FollowsImportWorker", []string{igUserIDString, igToken})
							log.Error("FollowersImportWorker: JUST IMPORTED FOLLOWS FOR A RELATIONSHIP THAT HASNT BEEN IMPORETED YET")
							workers.Enqueue("instagramediaimportworker", "MediaImportWorker", []string{igUserIDString, igToken})
						}
					}
				} else {
					// These are new nodes in Neo4j, Import their data
					if r.Body.(map[string]interface{})["data"] != nil && r.Body.(map[string]interface{})["data"].(map[string]interface{}) != nil {
						data, _ := r.Body.(map[string]interface{})["data"].(map[string]interface{})
						if data["InstagramID"] != nil {
							dataInstagramID, _ := data["InstagramID"].(string)
							log.Info("FollowersImportWorker: GOING TO GET THEIR dataInstagramID %v ", dataInstagramID)
							workers.Enqueue("instagramediaimportworker", "MediaImportWorker", []string{dataInstagramID, igToken})

							log.Info("FollowersImportWorker: DATA: %v", data)
							workers.Enqueue("instagramfollowsimportworker", "FollowsImportWorker", []string{dataInstagramID, igToken})
						}
					}
				}
			}
		}

	}
}

// Retry due to IG API Rate Limit or another Error
func performFollowersAgain(igUID string, igToken string) {
	log.Error("FollowersImportWorker: Retrying Due To Error")
	workers.EnqueueIn("FollowersImportWorker", "FollowersImportWorker", 3600.0, []string{igUID, igToken, "", "", string(6)})
}
