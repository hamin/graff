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

func FollowsImportWorker(message *workers.Msg) {

	igUID, igUIDErr := message.Args().GetIndex(0).String()
	log.Info("Starting FollowsImportWorker Import process: ", igUID)

	igToken, igTokenErr := message.Args().GetIndex(1).String()
	log.Info("Starting FollowsImportWorker Import process: ", igToken)

	cursorString, cursorErr := message.Args().GetIndex(2).String()
	cursor, _ := strconv.Atoi(cursorString)

	// userNeoNodeIDRaw, userNeoNodeIDErr := message.Args().GetIndex(3).String()
	// userNeoNodeID, _ := strconv.Atoi(userNeoNodeIDRaw)

	// if userNeoNodeIDRaw == "" {
	// 	log.Error("FollowsImportWorker: Shit is broken!!! ", userNeoNodeIDRaw)
	// 	return
	// }

	if igUIDErr != nil {
		log.Error("FollowsImportWorker: Missing IG User ID")
		return
	}

	if igTokenErr != nil {
		log.Error("FollowsImportWorker: Mssing IG Token")
		return
	}

	// if userNeoNodeIDErr != nil {
	// 	log.Error("FollowsImportWorker: Missing IG User Neo Node ID")
	// 	return
	// }

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	opt := &instagram.Parameters{Count: 20}

	if (cursorErr == nil) && (cursor > 0) {
		log.Info("FollowsImportWorker: We have a Cursor")
		opt.Cursor = uint64(cursor)
	}

	users, next, err := client.Relationships.Follows(igUID, opt)
	if err != nil {
		log.Error("FollowsImportWorker:", err)
		log.Error("FollowsImportWorker: Enqueing back in 1 hour")
		// if next.Cursor != "" {
		// 	workers.EnqueueIn("instagramfollowsimportworker", "FollowsImportWorker", 3600.0, []string{igUID, igToken, next.Cursor, userNeoNodeIDRaw})
		// } else {
		// 	workers.EnqueueIn("instagramfollowsimportworker", "FollowsImportWorker", 3600.0, []string{igUID, igToken, "", userNeoNodeIDRaw})
		// }
		performFollowsAgain(igUID, igToken, cursorString)
		return
	}
	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)
	batch := neo4jConnection.NewBatch()

	// batchOperations := []*neo4j.ManuelBatchRequest{}
	// var nodeIdx int
	// nodeIdx = 0
	//now we need to iterate trhough users and check if the user exists on neo4j, if user exists we just create
	//a relationship with the main node
	for _, u := range users {
		fmt.Printf("FollowsImportWorker: ID: %v, Username: %v\n", u.ID, u.Username)
		// Query if we already have imported user to Neo
		query := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' return id(c), c.MediaDataImportStarted, c.MediaDataImportFinished", u.ID)
		//log.Info("FollowsImportWorker: THIS IS IG USER CYPHER QUERY: %v ", query) // Confirm this Cypher Query

		response, _ := neohelpers.FindUserByCypher(neo4jConnection, query)
		log.Info("FollowsImportWorker: exstingIGUserNeoNodeID: ", response)

		if len(response) > 0 {
			userResponse, ok := response[0].([]interface{})
			if userResponse[0] != nil && ok {
				log.Info("FollowsImportWorker: userResponse", userResponse)
				// currentUserNodeIdRaw, _ := userResponse[0].(float64)
				// log.Info("FollowsImportWorker: currentUserNodeIdRaw: ", currentUserNodeIdRaw)
				// log.Info("FollowsImportWorker: userNeoNodeID: ", userNeoNodeID)
				//log.Info("FollowsImportWorker: currentUserNodeId", currentUserNodeId) // Confirm this Cypher Query
				unique := &neo4j.Unique{}
				unique.IndexName = "igpeople"
				unique.Key = "InstagramID"
				unique.Value = u.ID
				batch.Create(neohelpers.CreateCypherRelationshipOperationFrom(igUID, unique, "instagram_follows"))
				//neohelpers.AddRelationshipOperation(&batchOperations, int(userNeoNodeID), int(currentUserNodeIdRaw), true, true, "instagram_follows")
			}
		} else {
			log.Info("FollowsImportWorker: We should create this user on neo!")
			node := &neo4j.Node{}
			igNeoUser := User{}
			igNeoUser.InstagramID = u.ID
			igNeoUser.FullName = u.FullName
			igNeoUser.ProfilePicture = u.ProfilePicture
			igNeoUser.Username = u.Username
			igNeoUser.MediaDataImportStarted = false

			node.Data = structs.Map(igNeoUser)

			unique := &neo4j.Unique{}
			unique.IndexName = "igpeople"
			unique.Key = "InstagramID"
			unique.Value = u.ID

			batch.CreateUnique(node, unique)
			batch.Create(neohelpers.CreateCypherLabelOperation(unique, ":InstagramUser"))
			batch.Create(neohelpers.CreateCypherRelationshipOperationFrom(igUID, unique, "instagram_follows"))
			//neohelpers.AddLabelOperation(&batchOperations, nodeIdx, "InstagramUser")
			//neohelpers.AddRelationshipOperation(&batchOperations, int(userNeoNodeID), nodeIdx, true, false, "instagram_follows")
			//nodeIdx++
		}
	}

	// for _, batchOp := range batchOperations {
	// 	batch.Create(batchOp)
	// }

	res, err := batch.Execute()
	if err != nil {
		log.Error("FollowsImportWorker: THERE WAS AN ERROR EXECUTING BATCH!!!!")
		log.Error(err)
		log.Error(res)
		log.Error("FollowsImportWorker: IGUID: %v", igUID)
		performFollowsAgain(igUID, igToken, cursorString)
		return
	} else {
		log.Info("FollowsImportWorker: Successfully imported Media to Neo4J")
		if next.Cursor != "" {
			log.Info("FollowsImportWorker: *** This is our next.Cursor ", next.Cursor)
			// workers.Enqueue("instagramfollowsimportworker", "FollowsImportWorker", []string{igUID, igToken, next.Cursor, userNeoNodeIDRaw})
			workers.Enqueue("instagramfollowsimportworker", "FollowsImportWorker", []string{igUID, igToken, next.Cursor, ""})
			//log.Info("FollowsImportWorker: Sh Next Pagination Follows Import!!!")
		} else {
			log.Info("FollowsImportWorker: Done Importing Follows for IG User!")
		}
	}

}

// Retry due to IG API Rate Limit or another Error
func performFollowsAgain(igUID string, igToken string, cursorString string) {
	log.Error("instagramfollowsimportworker: Retrying Due To Error")
	if cursorString != "" {
		workers.EnqueueIn("instagramfollowsimportworker", "FollowsImportWorker", 3600.0, []string{igUID, igToken, cursorString, ""})
	} else {
		workers.EnqueueIn("instagramfollowsimportworker", "FollowsImportWorker", 3600.0, []string{igUID, igToken, "", ""})
	}
}
