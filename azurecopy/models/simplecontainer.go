package models

// SimpleContainer is AzureCopy's cloud agnostic version of a container
// SimpleContainers will NOT necessarily match real cloud provider definitions of
// containers.
//
// eg. Azure/S3 only have top level containers, but blobs inside of those can
// contain blobs with "virtual directory" like names.
// ie. container name is "foo" but blob name is "vdir1/vdir2/myblob"
// In this case we will end up with 3 SimpleContainers and 1 SimpleBlob.
//
// UNLESS I CHANGE MY MIND RANDOMLY, WHICH IS VERY POSSIBLE.
type SimpleContainer struct {
	Name   string
	URL    string
	Origin CloudType

	// slice of all blobs in this container
	BlobSlice []SimpleBlob

	// slice of all containers in this container
	ContainerSlice []SimpleContainer
}

// NewSimpleContainer factory time!
func NewSimpleContainer() *SimpleContainer {
	c := SimpleContainer{}
	c.BlobSlice = []SimpleBlob{}
	c.ContainerSlice = []SimpleContainer{}

	return &c
}

// GetContents retrieves contents of a container.
// populates blobSlice and containerSlice
func (c *SimpleContainer) GetContents(recursive bool) {

}
