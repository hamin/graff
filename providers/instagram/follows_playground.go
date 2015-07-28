package instagram

import (
	"fmt"
	// _ log "github.com/Sirupsen/logrus"
	_ "github.com/cihangir/neo4j"
	"github.com/maggit/go-instagram/instagram"
	"os"
)

func main() {
	testingToken := os.Getenv("INSTAGRAM_TESTING_ACCESS_TOKEN")
	client := instagram.NewClient(nil)
	client.AccessToken = testingToken

	opt := &instagram.Parameters{Count: 20}
	// opt.Cursor = 1435148200948
	// maxID, err := message.Args().GetIndex(1).String()
	// if (err == nil) && (maxID != "") {
	// 	log.Info("We have a Max ID")
	// 	opt.MaxID = maxID
	// }

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
	// log.Info(reflect.TypeOf(users))
	// log.Info(users)
}
