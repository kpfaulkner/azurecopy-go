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
}

// NewAzureCopy factory time!
func NewAzureCopy() *AzureCopy {
	ac := AzureCopy{}

	ac.BlobSlice = []models.SimpleBlob{}
	ac.ContainerSlice = []models.SimpleContainer{}

	return &ac
}

// GetRootContainer get the root container (and immediate containers/blobs)
func (ac *AzureCopy) GetRootContainer(cloudType models.CloudType, useEmulator bool) models.SimpleContainer {

	handler := utils.GetHandler(cloudType, useEmulator)
	rootContainer := handler.GetRootContainer()
	return rootContainer
}

// GetContainerContents populates the container with data.
func (ac *AzureCopy) GetContainerContents(container *models.SimpleContainer, cloudType models.CloudType, useEmulator bool) {
	handler := utils.GetHandler(cloudType, useEmulator)

	handler.GetContainerContents(container, useEmulator)

}
