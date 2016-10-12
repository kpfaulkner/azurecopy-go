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

	// GetSpecificSimpleContainer given a URL (ending in /) then get the SIMPLE container that represents it.
	GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error)

	// GetSpecificSimpleBlob given a URL (NOT ending in /) then get the SIMPLE blob that represents it.
	GetSpecificSimpleBlob(URL string) (*models.SimpleBlob, error)

	// given a container and a blob name, read the blob.
	ReadBlob(container models.SimpleContainer, blobName string) models.SimpleBlob

	// if we already have a reference to a SimpleBlob, then read it and populate it.
	PopulateBlob(blob *models.SimpleBlob) error

	// given a container and blob, write blob.
	WriteBlob(container *models.SimpleContainer, blob *models.SimpleBlob) error

	// write a container (and subcontents) to the appropriate data store
	WriteContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer) error

	// Gets a container. Populating the subtree? OR NOT? hmmmm
	GetContainer(containerName string) models.SimpleContainer

	// populates container with data.
	GetContainerContents(container *models.SimpleContainer, useEmulator bool)
}
