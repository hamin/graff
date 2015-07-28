package main

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/carbocation/go-instagram/instagram"
	"github.com/hamin/neo4j"
	// "github.com/jinzhu/gorm"
	"github.com/jrallison/go-workers"
	"os"
	"strconv"
	"time"
)

type instagramVenueImport struct {
	LocationID     int
	ExistsInDB     bool
	NeoVenueNodeID int32
	NodeIdx        int
}
