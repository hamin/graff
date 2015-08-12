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

// FollowsImportWorker - Imports an the users an Instagram User Follows to Neo4J
func FollowsImportWorker(message *workers.Msg) {

	igUID, igUIDErr := message.Args().GetIndex(0).String()
	igToken, igTokenErr := message.Args().GetIndex(1).String()

	cursorString, cursorErr := message.Args().GetIndex(2).String()
	cursor, _ := strconv.Atoi(cursorString)

	if igUIDErr != nil {
		log.Error("FollowsImportWorker: Missing IG User ID")
		return
	}

	if igTokenErr != nil {
		log.Error("FollowsImportWorker: Mssing IG Token")
		return
	}
	log.Info("Starting FollowsImportWorker Import process: ", igUID)

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	opt := &instagram.Parameters{Count: 20}

	if (cursorErr == nil) && (cursor > 0) {
		log.Info("FollowsImportWorker: We have a Cursor")
		opt.Cursor = uint64(cursor)
	}

	users, next, err := client.Relationships.Follows(igUID, opt)
	if err != nil {
		log.Error("FollowsImportWorker: Enqueing back in 1 hour ", err)
		performFollowsAgain(igUID, igToken, cursorString)
		return
	}
	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)
	batch := neo4jConnection.NewBatch()

	//Now we need to iterate through users and check if the user exists on neo4j, if user exists we just create
	//a relationship with the main node
	for _, u := range users {
		fmt.Printf("FollowsImportWorker: ID: %v, Username: %v\n", u.ID, u.Username)
		// Query if we already have imported user to Neo
		query := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' return id(c), c.MediaDataImportStarted, c.MediaDataImportFinished", u.ID)

		response, _ := neohelpers.FindUserByCypher(neo4jConnection, query)

		if len(response) > 0 {
			userResponse, ok := response[0].([]interface{})
			if userResponse[0] != nil && ok {
				unique := &neo4j.Unique{}
				unique.IndexName = "igpeople"
				unique.Key = "InstagramID"
				unique.Value = u.ID
				batch.Create(neohelpers.CreateCypherRelationshipOperationFrom(igUID, unique, "instagram_follows"))
			}
		} else {
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
		}
	}

	res, err := batch.Execute()
	if err != nil {
		log.Error("FollowsImportWorker: THERE WAS AN ERROR EXECUTING BATCH!!!!")
		log.Error(err)
		log.Error(res)
		log.Error("FollowsImportWorker: IGUID: %v", igUID)
		performFollowsAgain(igUID, igToken, cursorString)
		return
	}

	log.Info("FollowsImportWorker: Successfully imported Media to Neo4J")
	if next.Cursor != "" {
		log.Info("FollowsImportWorker: *** This is our next.Cursor ", next.Cursor)
		workers.Enqueue("instagramfollowsimportworker", "FollowsImportWorker", []string{igUID, igToken, next.Cursor})
	} else {
		log.Info("FollowsImportWorker: Done Importing Follows for IG User!")
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
