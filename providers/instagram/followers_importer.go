package instagram

import (
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

func FollowersImportWorker(message *workers.Msg) {
	igUID, igUIDErr := message.Args().GetIndex(0).String()
	log.Info("Starting FollowersImportWorker Import process: ", igUID)

	igToken, igTokenErr := message.Args().GetIndex(1).String()
	log.Info("Starting FollowersImportWorker Import process: ", igToken)

	cursorString, cursorErr := message.Args().GetIndex(2).String()
	cursor, _ := strconv.Atoi(cursorString)

	userNeoNodeIDRaw, userNeoNodeIDErr := message.Args().GetIndex(3).String()
	userNeoNodeID, _ := strconv.Atoi(userNeoNodeIDRaw)

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

	if userNeoNodeIDErr != nil {
		log.Error("FollowersImportWorker: Missing IG User Neo Node ID")
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

	if (cursorErr == nil) && (cursor > 0) {
		log.Info("We have a Cursor")
		opt.Cursor = uint64(cursor)
	}

	users, _, err := client.Relationships.FollowedBy("", opt)
	if err != nil {
		log.Error("FollowersImportWorkerError:", err)
		workers.EnqueueIn("followersimportworker", "FollowersImportWorker", 3600.0, []string{igUID, igToken, "", "", string(6)})
		return
	}

	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)
	batch := neo4jConnection.NewBatch()

	batchOperations := []*neo4j.ManuelBatchRequest{}

	var nodeIdx int
	nodeIdx = 0

	for _, u := range users {
		log.Info("ID: %v, Username: %v\n", u.ID, u.Username)
		// // Query if we already have imported user to Neo
		// query := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' return c", u.ID)
		// log.Info("FollowersImportWorker: THIS IS IG USER CYPHER QUERY: %v", query) // Confirm this Cypher Query
		// neoExistingUser, neoExistingUserErr := neohelpers.FindIDByCypher(neo4jConnection, query)

		// Query if we already have imported user to Neo
		query := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' return id(c), c.MediaDataImportStarted, c.MediaDataImportFinished", u.ID)
		//log.Info("FollowersImportWorker: THIS IS IG USER CYPHER QUERY: %v ", query) // Confirm this Cypher Query

		response, _ := neohelpers.FindUserByCypher(neo4jConnection, query)
		//log.Info("FollowersImportWorker: exstingIGUserNeoNodeID: ", response)

		if len(response) > 0 {
			// User Exists just Add Neo Relationship
			userResponse, ok := response[0].([]interface{})
			if userResponse[0] != nil && ok {
				currentUserNodeId, _ := userResponse[0].(int)
				//log.Info("FollowersImportWorker: currentUserNodeId", currentUserNodeId)
				neohelpers.AddRelationshipOperation(&batchOperations, currentUserNodeId, int(userNeoNodeID), true, true, "instagram_follows")
				//log.Info("FollowersImportWorker: we should check if MediaDataImported is true or enque media import for this user", currentUserNodeId)
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
			batch.Create(node)
			neohelpers.AddLabelOperation(&batchOperations, nodeIdx, "InstagramUser")
			neohelpers.AddRelationshipOperation(&batchOperations, nodeIdx, int(userNeoNodeID), false, true, "instagram_follows")
			nodeIdx++
		}
	}

	for _, batchOp := range batchOperations {
		batch.Create(batchOp)
	}

	res, err := batch.Execute()
	log.Info("RESPONSE FROM NEO$J FOR FOLLOWERS IMPORTER!!!!")
	if err != nil {
		log.Error("FollowersImportWorker: THERE WAS AN ERROR EXECUTING BATCH!!!!")
		log.Error(err)
		log.Error(res)
	} else {
		log.Info("FollowersImportWorker: Successfully imported Media to Neo4J")
		for _, r := range res {
			if r.Body != nil && r.Body.(map[string]interface{})["data"] != nil && r.Body.(map[string]interface{})["data"].(map[string]interface{}) != nil {
				data, _ := r.Body.(map[string]interface{})["data"].(map[string]interface{})
				if data["InstagramID"] != nil {
					dataInstagramID, _ := data["InstagramID"].(string)
					metaData, _ := r.Body.(map[string]interface{})["metadata"].(map[string]interface{})
					metaDataNeoIDRaw, _ := metaData["id"].(float64)
					var metaDataNeoID = strconv.FormatFloat(metaDataNeoIDRaw, 'f', 0, 64)
					workers.Enqueue("instagramediaimportworker", "MediaImportWorker", []string{dataInstagramID, igToken, "", metaDataNeoID})
				}
			}
		}

	}
}
