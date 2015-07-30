package main

import (
	"./neo_helpers"
	"./providers/instagram"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/cihangir/neo4j"
	"github.com/fatih/structs"
	"github.com/maggit/go-instagram/instagram"
	"os"
)

func main() {
	userNeoNodeID, userNeoNodeIDErr := os.Getenv("NEO_NODE_ID").Int()
	testingToken := os.Getenv("INSTAGRAM_TESTING_ACCESS_TOKEN")
	client := instagram.NewClient(nil)
	client.AccessToken = testingToken

	opt := &instagram.Parameters{Count: 20}
	// opt.Cursor = something
	// maxID, err := message.Args().GetIndex(1).String()
	// if (err == nil) && (maxID != "") {
	// 	log.Info("We have a Max ID")
	// 	opt.MaxID = maxID
	// }

	users, next, err := client.Relationships.Follows("", opt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
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
		fmt.Printf("ID: %v, Username: %v\n", u.ID, u.Username)
		// Query if we already have imported user to Neo
		query := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' return id(c)", u.ID)
		log.Info("THIS IS IG USER CYPHER QUERY: %v", query) // Confirm this Cypher Query
		exstingIGUserNeoNodeID, neoExistingUserErr := neohelpers.FindByCypher(neo4jConnection, query)
		if neoExistingUserErr != nil {
			log.Info("We should create this user on neo")
			node := &neo4j.Node{}
			igNeoUser := User{}
			igNeoUser.InstagramID = u.ID
			igNeoUser.FullName = u.FullName
			igNeoUser.ProfilePicture = u.ProfilePicture
			igNeoUser.Username = u.Username

			node.Data = structs.Map(igNeoUser)
			batch.Create(node)
			//ADD LABEL FOR USER NODE WITH {INDEX} = nodeIdx

			// unique := &neo4j.Unique{}
			// unique.IndexName = "ig_user_uid"
			// unique.Key = "InstagramID"
			// unique.Value = fmt.Sprintf("iguser%s", u.ID)

			// batch.CreateUnique(node, unique)

			batch.Create(node)
			neohelpers.AddLabelOperation(&batchOperations, nodeIdx, "InstagramUser")
			neohelpers.AddRelationshipOperation(&batchOperations, int(userNeoNodeID), nodeIdx, true, false, "instagram_follows")

		} else {
			log.Info("exstingIGUserNeoNodeID", exstingIGUserNeoNodeID) // Confirm this Cypher Query
			neohelpers.AddRelationshipOperation(&batchOperations, int(userNeoNodeID), exstingIGUserNeoNodeID, true, true, "instagram_follows")
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
		if next.NextURL != "" {
			log.Info("*** This is our next.NextURL ", next.NextURL)
			//workers.Enqueue("instagramediaimportworker", "InstagramMediaImportWorker", []string{igUID, igToken, next.NextMaxID, string(userNeoNodeID)})
			log.Info("Sh Next Pagination Follows Import!!!")
		} else {
			log.Info("Done Importing Follows for IG User!")
		}
	}

}
