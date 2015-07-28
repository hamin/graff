package instagram

// User - IG User
type User struct {
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
