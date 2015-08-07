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

func FollowersImportWorker(message *workers.Msg) {
	igUID, igUIDErr := message.Args().GetIndex(0).String()
	log.Info("Starting FollowersImportWorker Import process: ", igUID)

	igToken, igTokenErr := message.Args().GetIndex(1).String()
	log.Info("Starting FollowersImportWorker Import process: ", igToken)

	cursorString, cursorErr := message.Args().GetIndex(2).String()
	cursor, _ := strconv.Atoi(cursorString)

	followersLimitString, followersLimitError := message.Args().GetIndex(4).String()
	followersLimit, _ := strconv.Atoi(followersLimitString)

	if igUIDErr != nil {
		log.Error("FollowersImportWorker: Missing IG User ID")
		return
	}

	if igTokenErr != nil {
		log.Error("FollowersImportWorker: Mssing IG Token")
		return
	}

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	opt := &instagram.Parameters{}

	if (followersLimitError == nil) && (followersLimit > 0) {
		log.Info("We have a Limit")
		opt.Count = uint64(followersLimit)
	} else {
		opt.Count = 6
	}

	// TODO: Use the cursor string for pagination
	if (cursorErr == nil) && (cursor > 0) {
		log.Info("We have a Cursor")
		opt.Cursor = uint64(cursor)
	}

	users, _, err := client.Relationships.FollowedBy(igUID, opt)
	if err != nil {
		log.Error("FollowersImportWorkerError:", err)
		// workers.EnqueueIn("instagramfollowersimportworker", "FollowersImportWorker", 3600.0, []string{igUID, igToken, "", userNeoNodeIDRaw, string(6)})
		performFollowersAgain(igUID, igToken)
		return
	}

	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)
	batch := neo4jConnection.NewBatch()

	// batchOperations := []*neo4j.ManuelBatchRequest{}

	// var nodeIdx int
	// nodeIdx = 0

	for _, u := range users {
		log.Info("FollowersImportWorker ID: %v, Username: %v\n", u.ID, u.Username)
		// Query if we already have imported user to Neo
		query := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' return id(c), c.MediaDataImportStarted, c.MediaDataImportFinished", u.ID)

		response, _ := neohelpers.FindUserByCypher(neo4jConnection, query)
		log.Info("FollowersImportWorker: exstingIGUserNeoNodeID: ", response)
		if len(response) > 0 {
			// User Exists just Add Neo Relationship
			userResponse, ok := response[0].([]interface{})
			if userResponse[0] != nil && ok {
				log.Info("FollowersImportWorker: userResponse", userResponse)
				unique := &neo4j.Unique{}
				unique.IndexName = "igpeople"
				unique.Key = "InstagramID"
				unique.Value = u.ID
				batch.Create(neohelpers.CreateCypherRelationshipOperationTo(igUID, unique, "instagram_follows"))
			}
		} else {
			// Create Neo User
			// Then add relationship
			log.Info("FollowersImportWorker: We should create this user on neo!")
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
		for _, r := range res {
			if r.Body != nil {
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

						if nodeReponse.Data["MediaDataImportStarted"] != true {
							// Let's make sure to Enquque Media+Follows Importers
							// Import Follows
							workers.Enqueue("instagramfollowsimportworker", "FollowsImportWorker", []string{nodeReponse.Data["InstagramID"].(string), igToken, "", ""})
							log.Error("FollowersImportWorker: JUST IMPORTED FOLLOWS FOR A RELATIONSHIP THAT HASNT BEEN IMPORETED YET")
							// TODO: Import Media
						}
					}

				} else {
					if r.Body.(map[string]interface{})["data"] != nil && r.Body.(map[string]interface{})["data"].(map[string]interface{}) != nil {
						data, _ := r.Body.(map[string]interface{})["data"].(map[string]interface{})
						if data["InstagramID"] != nil {
							dataInstagramID, _ := data["InstagramID"].(string)
							metaData, _ := r.Body.(map[string]interface{})["metadata"].(map[string]interface{})
							metaDataNeoIDRaw, _ := metaData["id"].(float64)
							var metaDataNeoID = strconv.FormatFloat(metaDataNeoIDRaw, 'f', 0, 64)
							log.Info("FollowersImportWorker: GOING TO GET THEIR dataInstagramID %v ", dataInstagramID)
							workers.Enqueue("instagramediaimportworker", "MediaImportWorker", []string{dataInstagramID, igToken, "", metaDataNeoID})
							log.Info("FollowersImportWorker: GOING TO GET THEIR FOLLOWERS %v ", metaDataNeoID)

							log.Info("FollowersImportWorker: DATA: %v", data)
							workers.Enqueue("instagramfollowsimportworker", "FollowsImportWorker", []string{dataInstagramID, igToken, "", ""})
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
