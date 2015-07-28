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
)

// InstagramUser - IG User
type InstagramUser struct {
	InstagramID     string
	Username        string
	FullName        string
	ProfilePicture  string
	Bio             string
	Website         string
	MediaCount      int
	FollowsCount    int
	FollowedByCount int
}

// InstagramUserImportWorker Imports Instagram media to Neo4J
func InstagramUserImportWorker(message *workers.Msg) {

	igUID, igUIDErr := message.Args().GetIndex(0).String()
	log.Info("Starting NeoMedia Import process: ", igUID)

	igToken, igTokenErr := message.Args().GetIndex(1).String()
	log.Info("Starting NeoMedia Import process: ", igToken)

	if igUIDErr != nil {
		log.Error("Missing IG User ID")
	}

	if igTokenErr != nil {
		log.Error("Mssing IG Token")
	}

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)

	// Query if we already have imported user to Neo
	query := fmt.Sprintf("match (c:InstagramUser) where c.instagram_id = '%v' return id(c)", igUID)
	log.Info("THIS IS IG USER CYPHER QUERY: %v", query) // Confirm this Cypher Query
	exstingIGUserNeoNodeID, neoExistingUserErr := FindByCypher(neo4jConnection, query)

	if neoExistingUserErr != nil {
		// Enqueue Media and User Follows importer
		workers.Enqueue("instagramediaimportworker", "InstagramMediaImportWorker", []string{igUID, igToken, "", string(exstingIGUserNeoNodeID)})
		return
	}

	// Get IG User and Create Neo Node
	igUser, igErr := client.Users.Get(igUID)
	if igErr != nil {
		log.Error("Error: %v\n", igErr)
		log.Error("No IG user found: ", igUID)
		log.Info("Instagram API Failed, Enqueuing User Import Again With IGUID: ", igUID)
		workers.Enqueue("instagramuserimportworker", "InstagramUserImportWorker", []string{igUID, igToken})
		return
	}

	batch := neo4jConnection.NewBatch()

	node := &neo4j.Node{}
	igNeoUser := InstagramUser{}
	igNeoUser.InstagramID = igUser.ID
	igNeoUser.FullName = igUser.FullName
	igNeoUser.Bio = igUser.Bio
	igNeoUser.ProfilePicture = igUser.ProfilePicture
	igNeoUser.Username = igUser.Username
	igNeoUser.Website = igUser.Website
	igNeoUser.MediaCount = igUser.Counts.Media
	igNeoUser.FollowsCount = igUser.Counts.Follows
	igNeoUser.FollowedByCount = igUser.Counts.FollowedBy

	node.Data = structs.Map(igNeoUser)
	batch.Create(node)

	manuelLabel := &neo4j.ManuelBatchRequest{}
	manuelLabel.To = "{0}/labels"
	manuelLabel.StringBody = "NeoUser"
	batch.Create(manuelLabel)
	var nodeIDInt int
	res, err := batch.Execute()
	if err != nil {
		log.Error("Failed to create Neo4J User Node: %v", err)
		log.Error(err)
		log.Error(res)
		return
	}

	firstSlice, _ := res[0].Body.(map[string]interface{})["metadata"]
	secondSlice, _ := firstSlice.(map[string]interface{})
	thirdPass, _ := secondSlice["id"].(float64)
	nodeIDInt = int(thirdPass)
	log.Info("Successfully imported to Neo4J %v", nodeIDInt)

	if err != nil {
		log.Error("Couldn't parse Node ID to INT")
		return
	}
	if nodeIDInt == 0 {
		log.Error("User node id shouldn't be 0")
		return
	}

	// Enqueue Media and Follows Importer for new Neo IG User
	workers.Enqueue("instagramediaimportworker", "InstagramMediaImportWorker", []string{igUID, igToken, "", string(nodeIDInt)})
	return
}