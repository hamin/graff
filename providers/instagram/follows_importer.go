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

	userNeoNodeID, userNeoNodeIDErr := message.Args().GetIndex(3).Int()
	if igUIDErr != nil {
		log.Error("FollowsImportWorker: Missing IG User ID")
	}

	if igTokenErr != nil {
		log.Error("FollowsImportWorker: Mssing IG Token")
	}

	if userNeoNodeIDErr != nil {
		log.Error("FollowsImportWorker: Missing IG User Neo Node ID")
	}

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	opt := &instagram.Parameters{Count: 20}

	if (cursorErr == nil) && (cursor > 0) {
		log.Info("FollowsImportWorker: We have a Cursor")
		opt.Cursor = uint64(cursor)
	}

	users, next, err := client.Relationships.Follows("", opt)
	if err != nil {
		log.Error("FollowsImportWorker:", err)
		log.Error("FollowsImportWorker: Enqueing back in 1 hour")
		workers.EnqueueIn("followsimportworker", "FollowsImportWorker", 3600.0, []string{igUID, igToken, "", string(userNeoNodeID)})
		return
	}
	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)
	batch := neo4jConnection.NewBatch()

	batchOperations := []*neo4j.ManuelBatchRequest{}
	var nodeIdx int
	nodeIdx = 0
	//now we need to iterate trhough users and check if the user exists on neo4j, if user exists we just create
	//a relationship with the main node
	for _, u := range users {
		fmt.Printf("FollowsImportWorker: ID: %v, Username: %v\n", u.ID, u.Username)
		// Query if we already have imported user to Neo
		query := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' return id(c)", u.ID)
		log.Info("FollowsImportWorker: THIS IS IG USER CYPHER QUERY: %v", query) // Confirm this Cypher Query
		exstingIGUserNeoNodeID, neoExistingUserErr := neohelpers.FindByCypher(neo4jConnection, query)
		if neoExistingUserErr != nil {
			log.Info("FollowsImportWorker: We should create this user on neo!")
			node := &neo4j.Node{}
			igNeoUser := User{}
			igNeoUser.InstagramID = u.ID
			igNeoUser.FullName = u.FullName
			igNeoUser.ProfilePicture = u.ProfilePicture
			igNeoUser.Username = u.Username

			node.Data = structs.Map(igNeoUser)
			batch.Create(node)

			// unique := &neo4j.Unique{}
			// unique.IndexName = "ig_user_uid"
			// unique.Key = "InstagramID"
			// unique.Value = fmt.Sprintf("iguser%s", u.ID)

			// batch.CreateUnique(node, unique)

			batch.Create(node)
			neohelpers.AddLabelOperation(&batchOperations, nodeIdx, "InstagramUser")
			neohelpers.AddRelationshipOperation(&batchOperations, int(userNeoNodeID), nodeIdx, true, false, "instagram_follows")

		} else {
			log.Info("FollowsImportWorker: exstingIGUserNeoNodeID", exstingIGUserNeoNodeID) // Confirm this Cypher Query
			neohelpers.AddRelationshipOperation(&batchOperations, int(userNeoNodeID), exstingIGUserNeoNodeID, true, true, "instagram_follows")
		}
		nodeIdx++
	}

	for _, batchOp := range batchOperations {
		batch.Create(batchOp)
	}

	res, err := batch.Execute()
	if err != nil {
		log.Error("FollowsImportWorker: THERE WAS AN ERROR EXECUTING BATCH!!!!")
		log.Error(err)
		log.Error(res)
	} else {
		log.Info("FollowsImportWorker: Successfully imported Media to Neo4J")
		if next.NextURL != "" {
			log.Info("FollowsImportWorker: *** This is our next.NextURL ", next)
			workers.Enqueue("followsimportworker", "FollowsImportWorker", []string{igUID, igToken, next.Cursor, string(userNeoNodeID)})
			log.Info("FollowsImportWorker: Sh Next Pagination Follows Import!!!")
		} else {
			log.Info("FollowsImportWorker: Done Importing Follows for IG User!")
		}
	}

}
