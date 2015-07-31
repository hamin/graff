package instagram

import (
	"../../neo_helpers"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/cihangir/neo4j"
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

	//_, userNeoNodeIDErr := message.Args().GetIndex(3).Int()

	followersLimitString, followersLimitError := message.Args().GetIndex(4).String()
	followersLimit, _ := strconv.Atoi(followersLimitString)

	if igUIDErr != nil {
		log.Error("FollowersImportWorker: Missing IG User ID")
	}

	if igTokenErr != nil {
		log.Error("FollowersImportWorker: Mssing IG Token")
	}

	// if userNeoNodeIDErr != nil {
	// 	log.Error("Missing IG User Neo Node ID")
	// }
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

	for _, u := range users {
		log.Info("ID: %v, Username: %v\n", u.ID, u.Username)
		// Query if we already have imported user to Neo
		query := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' return id(c)", u.ID)
		log.Info("FollowersImportWorker: THIS IS IG USER CYPHER QUERY: %v", query) // Confirm this Cypher Query
		_, neoExistingUserErr := neohelpers.FindByCypher(neo4jConnection, query)
		if neoExistingUserErr != nil {
			log.Info("FollowersImportWorker: We should create this user on neo, enqueue!")
			workers.Enqueue("instagramuserimportworker", "InstagramUserImportWorker", []string{u.ID, igToken})
		}
	}
}
