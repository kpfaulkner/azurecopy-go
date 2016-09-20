package azurecopy

import (
	"azurecopy/azurecopy/models"
)

// AzureCopy main client class.
type AzureCopy struct {

	// list of blobs.
	blobSlice []models.SimpleBlob

	// list of containers.
	containerSlice []models.SimpleContainer
}
