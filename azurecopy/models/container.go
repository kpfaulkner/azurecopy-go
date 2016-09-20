package container

type Container struct {
	name   string
	url    string
	origin CloudOrigin

	// slice of all blobs in this container
	blobSlice []Blob

	// slice of all containers in this container
	containerSlice []Container
}
