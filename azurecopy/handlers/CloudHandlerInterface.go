package handlers

import (
	"azurecopy/azurecopy/models"
)

// CloudHandlerInterface is the interface for all cloud based operations
// each cloud handler will implement these.
// list blobs/containers/read/write etc.
type CloudHandlerInterface interface {

	// gets root container. This will get containers/blobs in this container
	// NOT recursive.
	GetRootContainer() models.SimpleContainer

	// create container.
	CreateContainer(parentContainer models.SimpleContainer, containerName string) models.SimpleContainer

	// given a container and a blob name, read the blob.
	ReadBlob(container models.SimpleContainer, blobName string) models.SimpleBlob

	// given a container and blob, write blob.
	WriteBlob(container models.SimpleContainer, blob models.SimpleBlob)
}
