package constants

type CloudOrigin int

// fugly.
const (
	Azure CloudOrigin = 1 + iota
	S3
	DropBox
	OneDrive
	Filesystem
)