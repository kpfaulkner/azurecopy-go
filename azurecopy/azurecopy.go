package azurecopy

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils"
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

	handler := utils.GetHandler(cloudType, ac.UseEmulator)
	rootContainer := handler.GetRootContainer()
	return rootContainer
}

// GetContainerContents populates the container with data.
func (ac *AzureCopy) GetContainerContents(container *models.SimpleContainer) {
	handler := utils.GetHandler(container.Origin, ac.UseEmulator)
	handler.GetContainerContents(container, ac.UseEmulator)
}
