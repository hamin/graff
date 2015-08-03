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
	"reflect"
)

// UserImportWorker Imports Instagram media to Neo4J
func UserImportWorker(message *workers.Msg) {

	igUID, igUIDErr := message.Args().GetIndex(0).String()
	log.Info("UserImportWorker: Starting NeoMedia Import process: ", igUID)

	igToken, igTokenErr := message.Args().GetIndex(1).String()
	log.Info("UserImportWorker: Starting NeoMedia Import process: ", igToken)

	if igUIDErr != nil {
		log.Error("UserImportWorker: Missing IG User ID")
	}

	if igTokenErr != nil {
		log.Error("UserImportWorker: Mssing IG Token")
	}

	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)

	// Query if we already have imported user to Neo
	query := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' return id(c), c.MediaDataImportStarted, c.MediaDataImportFinished", igUID)
	log.Info("UserImportWorker: THIS IS IG USER CYPHER QUERY: %v ", query) // Confirm this Cypher Query

	response, _ := neohelpers.FindUserByCypher(neo4jConnection, query)
	log.Info("UserImportWorker: exstingIGUserNeoNodeID: ", response)

	if len(response) > 0 {
		log.Info("If the user exists don't create user, check if the import finished or started", response)
		log.Info(reflect.TypeOf(response))
		userResponse, ok := response[0].([]interface{})
		if userResponse[1] == true && ok {
			log.Info("UserImportWorker: Nothing to do, we have user and media.")
			return
		}
		if userResponse[0] != nil && ok {
			log.Info("UserImportWorker: Importing media for already created user")
			currentUserNodeId, _ := userResponse[0].(string)
			updateQuery := fmt.Sprintf("match (c:InstagramUser) where id(c)= %v SET c.MediaDataImportStarted=true", userResponse[0])
			log.Info("UserImportWorker: QUERY: ", updateQuery)
			response, updateUserError := neohelpers.UpdateNodeWithCypher(neo4jConnection, updateQuery)
			if updateUserError == nil {
				log.Info("UserImportWorker: UPDATING NODE WITH CYPHER: ", response)
				workers.Enqueue("instagramediaimportworker", "MediaImportWorker", []string{igUID, igToken, "", currentUserNodeId})
				return
			}
			log.Error("UserimportWorker: error updating user", updateUserError)
		}
		log.Error("UserImportWorker: User data didn't get imported and won't get imported")
		return
	}

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	// Get IG User and Create Neo Node
	igUser, igErr := client.Users.Get(igUID)
	if igErr != nil {
		log.Error("UserImportWorker: Error: %v\n", igErr)
		log.Error("UserImportWorker: No IG user found: ", igUID)
		log.Info("UserImportWorker: Instagram API Failed, Enqueuing User Import Again With IGUID: ", igUID)
		workers.Enqueue("instagramuserimportworker", "UserImportWorker", []string{igUID, igToken})
		return
	}

	batch := neo4jConnection.NewBatch()

	node := &neo4j.Node{}
	igNeoUser := User{}
	igNeoUser.InstagramID = igUser.ID
	igNeoUser.FullName = igUser.FullName
	igNeoUser.Bio = igUser.Bio
	igNeoUser.ProfilePicture = igUser.ProfilePicture
	igNeoUser.Username = igUser.Username
	igNeoUser.Website = igUser.Website
	igNeoUser.MediaCount = igUser.Counts.Media
	igNeoUser.FollowsCount = igUser.Counts.Follows
	igNeoUser.FollowedByCount = igUser.Counts.FollowedBy
	igNeoUser.MediaDataImportStarted = true
	node.Data = structs.Map(igNeoUser)
	batch.Create(node)

	manuelLabel := &neo4j.ManuelBatchRequest{}
	manuelLabel.To = "{0}/labels"
	manuelLabel.StringBody = "InstagramUser"
	batch.Create(manuelLabel)
	var nodeIDInt int
	res, err := batch.Execute()
	if err != nil {
		log.Error("UserImportWorker: Failed to create Neo4J User Node: %v", err)
		log.Error(err)
		log.Error(res)
		return
	}

	firstSlice, _ := res[0].Body.(map[string]interface{})["metadata"]
	secondSlice, _ := firstSlice.(map[string]interface{})
	thirdPass, _ := secondSlice["id"].(float64)
	nodeIDInt = int(thirdPass)
	log.Info("UserImportWorker: Successfully imported to Neo4J %v ", nodeIDInt)

	if err != nil {
		log.Error("UserImportWorker: Couldn't parse Node ID to INT")
		return
	}

	//Enqueue Media and Follows Importer for new Neo IG User
	//workers.Enqueue("instagramediaimportworker", "MediaImportWorker", []string{igUID, igToken, "", string(nodeIDInt)})
	//Enqueue Follows Importer for new Neo IG User
	workers.Enqueue("instagramfollowsimportworker", "FollowsImportWorker", []string{igUID, igToken, "", string(nodeIDInt)})

	//Enqueue Recent Followers
	//workers.Enqueue("followersimportworker", "FollowersImportWorker", []string{igUID, igToken, "", "", string(6)})
	return
}
