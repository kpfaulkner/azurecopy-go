package models

// SimpleBlob is AzureCopy's cloud agnostic version of a blob
// Although real clouds (Azure/S3 etc) allow blob names to simulate virtual directories
// ie blob name can be "vdir1/vdir2/myblob" we will only store the "file" part of the URL.
// so in this case it would be "myblob" and the containing container would be "vdir2" which
// would also have a parent container "vdir1" etc.
type SimpleBlob struct {

	// data, if cached in memory
	DataInMemory []byte

	// path to cached version of blob on disk.
	DataCachedAtPath string

	// if true then DataInMemory contains blob data
	// else DataCachedAtPath contains path to on disk cache of blob.
	BlobInMemory bool

	// name of blob. Generic nice stuff. (ie no fake vdirs)
	Name string
	URL  string

	// destination blob name
	DestName string

	// REAL platform (Azure, S3 etc) name of blob.
	// ie including the nasty vdirs etc.
	BlobCloudName string

	Origin CloudType

	// indicates if this container was read from the source or destination.
	IsSource bool

	// parent.
	ParentContainer *SimpleContainer
}
