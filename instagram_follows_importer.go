package main

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

type InstagramResponse struct {
	Pagination *InstagramPagination `json:"pagination,omitempty"`
	Data       []InstagramUser      `json:"data,omitempty"`
}

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

type InstagramPagination struct {
	NextUrl    string `json:"next_url,omitempty"`
	NextCursor string `json:"next_cursor,omitempty"`
}

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
