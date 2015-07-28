package instagram

import (
	_ "errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "github.com/cihangir/neo4j"
	"github.com/jrallison/go-workers"
	"github.com/maggit/go-instagram/instagram"
	"os"
	"strconv"
	_ "time"
)

func InstagramFollowsImporter(message *workers.Msg) {

	igUID, _ := message.Args().GetIndex(0).String()
	log.Info("Starting NeoMedia Import process: ", igUID)

	igToken, _ := message.Args().GetIndex(1).String()
	log.Info("Starting NeoMedia Import process: ", igToken)

	cursorString, cursorErr := message.Args().GetIndex(2).String()
	cursor, _ := strconv.Atoi(cursorString)

	client := instagram.NewClient(nil)
	client.AccessToken = igToken

	opt := &instagram.Parameters{Count: 20}

	if (cursorErr == nil) && (cursor > 0) {
		log.Info("We have a Cursor")
		opt.Cursor = uint64(cursor)
	}

	users, next, err := client.Relationships.Follows("", opt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	}
	for _, u := range users {
		fmt.Printf("ID: %v, Username: %v\n", u.ID, u.Username)
	}
	if next.NextURL != "" {
		fmt.Println("Next URL", next.NextURL)
	}

}
