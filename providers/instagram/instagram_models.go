package instagram

import "strings"

// User - IG User
type User struct {
	InstagramID             string
	Username                string
	FullName                string
	ProfilePicture          string
	Bio                     string
	Website                 string
	MediaCount              int
	FollowsCount            int
	FollowedByCount         int
	MediaDataImportStarted  bool
	MediaDataImportFinished bool
}

// MediaItem - This is media object from IG
type MediaItem struct {
	Type                string
	Filter              string
	Link                string
	UserHasLiked        bool
	CreatedTime         int64
	InstagramID         string
	ImageThumbnail      string
	ImageLowResolution  string
	ImageHighResolution string
	VideoLowResolution  string
	VideoHighResolution string
	CaptionID           string
	CaptionText         string
	LikesCount          int
	HasVenue            bool
}

// MediaLocation - This is media object from IG
type MediaLocation struct {
	InstagramID string
	Name        string
	Latitude    float64
	Longitude   float64
}

// CheckIGErrorForUnavailableResource - Kind of hacky, but checks if error from IG is for a private/unavailable resource
func CheckIGErrorForUnavailableResource(err error) bool {
	s := err.Error()
	return strings.Contains(s, "APINotAllowedError") || strings.Contains(s, "400") || strings.Contains(s, "cannot view this resource")
}
