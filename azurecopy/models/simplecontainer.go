package models

import (
	"errors"
	"fmt"
)

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
	BlobSlice []*SimpleBlob

	// slice of all containers in this container
	ContainerSlice []*SimpleContainer

	// have we attempted to populate this container?
	Populated bool
}

// NewSimpleContainer factory time!
func NewSimpleContainer() *SimpleContainer {
	c := SimpleContainer{}
	c.BlobSlice = []*SimpleBlob{}
	c.ContainerSlice = []*SimpleContainer{}
	c.ParentContainer = nil
	c.Populated = false
	return &c
}

// GetBlob gets a reference to a blob in the container. Does NOT go recursive, recursive, recursive, recursive......
func (sc *SimpleContainer) GetBlob(blobName string) (*SimpleBlob, error) {

	for _, b := range sc.BlobSlice {
		if b.Name == blobName {
			return b, nil
		}
	}
	err := errors.New("Blob " + blobName + " not found")
	return nil, err
}

// GetBlob gets a reference to a blob in the container. Does NOT go recursive, recursive, recursive, recursive......
func (sc *SimpleContainer) GetContainer(containerName string) (*SimpleContainer, error) {

	for _, c := range sc.ContainerSlice {
		if c.Name == containerName {
			return c, nil
		}
	}
	err := errors.New("Container " + containerName + " not found")
	return nil, err
}

func (sc *SimpleContainer) DisplayContainer(padding string) {

	fmt.Println("+" + padding + sc.Name)

	padding = padding + "  "

	for _, b := range sc.BlobSlice {
		fmt.Println(padding + b.Name + "(" + b.URL + ")")
	}

	for _, c := range sc.ContainerSlice {
		c.DisplayContainer(padding)
	}
}
