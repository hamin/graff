package instagram

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

// InstagramMediaItem - This is media object from IG
type InstagramMediaItem struct {
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

// InstagramMediaLocation - This is media object from IG
type InstagramMediaLocation struct {
	InstagramID string
	Name        string
	Latitude    float64
	Longitude   float64
}
