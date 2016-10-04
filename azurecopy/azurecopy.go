package azurecopy

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils"
	"log"
)

// AzureCopy main client class.
type AzureCopy struct {

	// list of blobs.
	BlobSlice []models.SimpleBlob

	// list of containers.
	ContainerSlice []models.SimpleContainer

	// are we using an emualtor?
	UseEmulator bool
}

// NewAzureCopy factory time!
func NewAzureCopy(useEmulator bool) *AzureCopy {
	ac := AzureCopy{}

	ac.BlobSlice = []models.SimpleBlob{}
	ac.ContainerSlice = []models.SimpleContainer{}
	ac.UseEmulator = useEmulator

	return &ac
}

// GetRootContainer get the root container (and immediate containers/blobs)
func (ac *AzureCopy) GetRootContainer(cloudType models.CloudType) models.SimpleContainer {

	handler := utils.GetHandler(cloudType, ac.UseEmulator, true)
	rootContainer := handler.GetRootContainer()
	return rootContainer
}

// GetContainerContents populates the container with data.
func (ac *AzureCopy) GetContainerContents(container *models.SimpleContainer) {
	handler := utils.GetHandler(container.Origin, ac.UseEmulator, true)
	handler.GetContainerContents(container, ac.UseEmulator)
}

// ReadBlob reads a blob and keeps it in memory OR caches to disk.
// (or in the special case of azure copyblob flag it will do something tricky, once I get to that part)
func (ac *AzureCopy) ReadBlob(blob *models.SimpleBlob) {

	log.Println("ReadBlob " + blob.Name)
	handler := utils.GetHandler(blob.Origin, ac.UseEmulator, true)
	err := handler.PopulateBlob(blob)

	if err != nil {
		log.Fatal(err)

	}
}
