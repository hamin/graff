package main

import (
	"fmt"
	// _ log "github.com/Sirupsen/logrus"
	_ "github.com/cihangir/neo4j"
	"github.com/maggit/go-instagram/instagram"
	"os"
)

// User represents Instagram user.
type InstagramUser struct {
	ID             string     `json:"id,omitempty"`
	Username       string     `json:"username,omitempty"`
	FullName       string     `json:"full_name,omitempty"`
	ProfilePicture string     `json:"profile_picture,omitempty"`
	Bio            string     `json:"bio,omitempty"`
	Website        string     `json:"website,omitempty"`
	Counts         *UserCount `json:"counts,omitempty"`
}

// UserCount represents stats of a Instagram user.
type UserCount struct {
	Media      int `json:"media,omitempty"`
	Follows    int `json:"follows,omitempty"`
	FollowedBy int `json:"followed_by,omitempty"`
}

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
