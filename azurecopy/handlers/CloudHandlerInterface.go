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
	CreateContainer(containerName string) (models.SimpleContainer, error)

	// GetSpecificSimpleContainer given a URL (ending in /) then get the SIMPLE container that represents it.
	GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error)

	// GetContainerContentsOverChannel given a URL (ending in /) returns all the contents of the container over a channel
	// This is going to be inefficient from a memory allocation pov.
	// Am still creating various structs that we strictly do not require for copying (all the tree structure etc) but this will
	// at least help each cloud provider be consistent from a dev pov. Think it's worth the overhead. TODO(kpfaulkner) confirm :)
	GetContainerContentsOverChannel(sourceContainer models.SimpleContainer, blobChannel chan models.SimpleContainer) error

	// GetSpecificSimpleBlob given a URL (NOT ending in /) then get the SIMPLE blob that represents it.
	GetSpecificSimpleBlob(URL string) (*models.SimpleBlob, error)

	// Given a container and a blob name, read the blob.
	ReadBlob(container models.SimpleContainer, blobName string) models.SimpleBlob

	// Does blob exist
	BlobExists(container models.SimpleContainer, blobName string) (bool, error)

	// if we already have a reference to a SimpleBlob, then read it and populate it.
	PopulateBlob(blob *models.SimpleBlob) error

	// given a container and blob, write blob.
	WriteBlob(container *models.SimpleContainer, blob *models.SimpleBlob) error

	// write a container (and subcontents) to the appropriate data store
	WriteContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer) error

	// Gets a container. Populating the subtree? OR NOT? hmmmm
	GetContainer(containerName string) models.SimpleContainer

	// populates container with data.
	GetContainerContents(container *models.SimpleContainer) error
}
