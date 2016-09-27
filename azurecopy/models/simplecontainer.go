package models

import "fmt"

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

	// parent.
	// if nil parent then its the root.
	ParentContainer *SimpleContainer

	// slice of all blobs in this container
	BlobSlice []SimpleBlob

	// slice of all containers in this container
	ContainerSlice []SimpleContainer

	//Handler *CloudHandlerInterface

}

// NewSimpleContainer factory time!
func NewSimpleContainer() *SimpleContainer {
	c := SimpleContainer{}
	c.BlobSlice = []SimpleBlob{}
	c.ContainerSlice = []SimpleContainer{}
	c.ParentContainer = nil
	return &c
}

func (sc *SimpleContainer) DisplayContainer(padding string) {

	fmt.Println(padding + sc.Name)

	padding = padding + "  "

	for _, b := range sc.BlobSlice {
		fmt.Println(padding + b.Name)
	}

	for _, c := range sc.ContainerSlice {
		c.DisplayContainer(padding)
	}
}
