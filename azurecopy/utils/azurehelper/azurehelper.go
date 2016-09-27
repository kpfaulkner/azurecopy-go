package azurehelper

import "azurecopy/azurecopy/models"

// GetAzureContainer Gets the REAL Azure container and the blob prefix for a given SimpleContainer
// that has been passed in.
func GetContainerAndBlobPrefix(container *models.SimpleContainer) (*models.SimpleContainer, string) {
	var p *models.SimpleContainer
	blobPrefix := ""
	var azureContainer *models.SimpleContainer

	for p = container; p != nil; {

		// if parent container is not nil, then we're NOT a real azure container.
		if p.ParentContainer != nil {
			blobPrefix = p.Name + "/" + blobPrefix
		} else {
			// parent IS nil, therefore we're in the real azure container.
			azureContainer = p
		}

		p = p.ParentContainer

	}
	return azureContainer, blobPrefix
}
