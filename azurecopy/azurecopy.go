package azurecopy

import (
	"azurecopy/azurecopy/handlers"
	"azurecopy/azurecopy/models"
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

// GetHandler gets the appropriate handler for the cloudtype.
// Should I be doing this another way?
func (ac *AzureCopy) GetHandler(cloudType models.CloudType) handlers.CloudHandlerInterface {
	switch cloudType {
	case models.Azure:
		ah := handlers.NewAzureHandler()
		return ah

	case models.Filesystem:
		fh := handlers.NewFilesystemHandler()
		return fh
	}

	return nil
}
