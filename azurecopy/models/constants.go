package models

type CloudOrigin int

// fugly.
const (
	OriginAzure CloudOrigin = 1 + iota
	OriginS3
	OriginDropBox
	OriginOneDrive
	OriginFilesystem
)
