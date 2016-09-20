package models

// SimpleBlob is AzureCopy's cloud agnostic version of a blob
// Although real clouds (Azure/S3 etc) allow blob names to simulate virtual directories
// ie blob name can be "vdir1/vdir2/myblob" we will only store the "file" part of the URL.
// so in this case it would be "myblob" and the containing container would be "vdir2" which
// would also have a parent container "vdir1" etc.
type SimpleBlob struct {

	// data.
	data   []byte
	name   string
	url    string
	origin CloudOrigin
}
