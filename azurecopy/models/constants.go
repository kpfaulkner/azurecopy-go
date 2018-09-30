package models

type CloudType int

// fugly.
const (
	Azure CloudType = 1 + iota
	S3
	DropBox
	OneDrive
	Filesystem
	FTP
)
