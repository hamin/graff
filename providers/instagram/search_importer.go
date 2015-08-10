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
	// "strconv"
)

// SearchImportWorker - Import IG Users that ppl are Searching for but are not in Graff
func SearchImportWorker(message *workers.Msg) {
	igUsername, igUsernameErr := message.Args().GetIndex(0).String()
	log.Info("Starting SearchImportWorker Import process: ", igUsername)

	igToken, igTokenErr := message.Args().GetIndex(1).String()
	log.Info("Starting SearchImportWorker Import process: ", igToken)

	if igUsernameErr != nil {
		log.Error("SearchImportWorker: Missing IG User ID")
		return
	}

	if igTokenErr != nil {
		log.Error("SearchImportWorker: Mssing IG Token")
		return
	}

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	opt := &instagram.Parameters{Count: 1}

	users, _, err := client.Users.Search(igUsername, opt)
	if err != nil {
		log.Error("SearchImportWorker:", err)
		// TODO Perform Again maybe? or let it be?
		return
	}

	if len(users) == 0 {
		log.Error("SearchImportWorker: IG USER NOT FOUND: %v", igUsername)
		return
	}

	igUser := users[0]

	neoHost := os.Getenv("NEO4JURI")
	neo4jConnection := neo4j.Connect(neoHost)

	query := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' return id(c), c.MediaDataImportStarted, c.MediaDataImportFinished", igUser.ID)
	response, _ := neohelpers.FindUserByCypher(neo4jConnection, query)
	log.Info("SearchImportWorker: FindUserByCypher resp: ", response)

	if len(response) > 0 {
		// NEO User Exists, check if their media + follows is imported
		userResponse, ok := response[0].([]interface{})
		if userResponse[1] != true && ok {
			log.Info("SearchImportWorker: User is In Neo But Media Import Not started: %v", igUsername)
			startMediaAndFollowsWorker(igUser.ID, igToken)
		}
		updateQuery := fmt.Sprintf("match (c:InstagramUser) where c.InstagramID = '%v' SET c.MediaDataImportStarted=true RETURN c", igUser.ID)
		updateResponse, updateUserError := neohelpers.UpdateNodeWithCypher(neo4jConnection, updateQuery)
		if updateUserError == nil {
			log.Info("SearchImportWorker: UPDATING NODE WITH CYPHER successfully MediaDataImportStarted=true", updateResponse)
		} else {
			log.Error("SearchImportWorker: error updating user MediaDataImportStarted :(", updateUserError)
		}
		return
	}

	// Otherwise let's Create and Import this user

	// We have to featch the user's entire object from IG
	igFetchedUser, igErr := client.Users.Get(igUser.ID)
	if igErr != nil {
		log.Error("SearchImportWorker: IG User Fetch Error: %v\n", igErr)
		log.Error("SearchImportWorker: No IG user found: ", igUser.ID)
		return
	}
	log.Info("SearchImportWorker: We should create this user on neo!")
	batch := neo4jConnection.NewBatch()

	node := &neo4j.Node{}
	igNeoUser := User{}
	igNeoUser.InstagramID = igFetchedUser.ID
	igNeoUser.FullName = igFetchedUser.FullName
	igNeoUser.Bio = igFetchedUser.Bio
	igNeoUser.ProfilePicture = igFetchedUser.ProfilePicture
	igNeoUser.Username = igFetchedUser.Username
	igNeoUser.Website = igFetchedUser.Website
	igNeoUser.MediaCount = igFetchedUser.Counts.Media
	igNeoUser.FollowsCount = igFetchedUser.Counts.Follows
	igNeoUser.FollowedByCount = igFetchedUser.Counts.FollowedBy
	igNeoUser.MediaDataImportStarted = true
	node.Data = structs.Map(igNeoUser)

	unique := &neo4j.Unique{}
	unique.IndexName = "igpeople"
	unique.Key = "InstagramID"
	unique.Value = igUser.ID

	batch.CreateUnique(node, unique)
	batch.Create(neohelpers.CreateCypherLabelOperation(unique, ":InstagramUser"))
	res, err := batch.Execute()
	if err != nil {
		log.Error("SearchImportWorker: THERE WAS AN ERROR EXECUTING BATCH!!!!")
		log.Error(err)
		log.Error(res)
		log.Error("SearchImportWorker: IGUSERNAME: %v", igUsername)
		performSearchAgain(igUsername, igToken)
		return
	}
	startMediaAndFollowsWorker(igFetchedUser.ID, igToken)
}

func performSearchAgain(igUsername string, igToken string) {
	workers.Enqueue("instagramsearchimportworker", "SearchImportWorker", []string{igUsername, igToken})
}

func startMediaAndFollowsWorker(igUserID string, igToken string) {
	//Enqueue Media and Follows Importer for new Neo IG User
	workers.Enqueue("instagramediaimportworker", "MediaImportWorker", []string{igUserID, igToken, "", ""})
	//Enqueue Follows Importer for new Neo IG User
	workers.Enqueue("instagramfollowsimportworker", "FollowsImportWorker", []string{igUserID, igToken, "", ""})
}
