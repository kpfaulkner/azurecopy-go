package containerutils

import (
	"azurecopy/azurecopy/models"

	log "github.com/Sirupsen/logrus"
)

// GetRootContainer Get root container of a simple container.
func GetRootContainer(container *models.SimpleContainer) *models.SimpleContainer {
	var p *models.SimpleContainer

	for p = container.ParentContainer; p.ParentContainer != nil; {
		p = p.ParentContainer
	}

	return p
}

// GetContainerAndBlobPrefix Gets the REAL Azure container and the blob prefix for a given SimpleContainer
// that has been passed in.
func GetContainerAndBlobPrefix(container *models.SimpleContainer) (*models.SimpleContainer, string) {
	var p *models.SimpleContainer
	blobPrefix := ""
	var realContainer *models.SimpleContainer

	for p = container; p != nil; {

		// if parent container is not nil, then we're NOT a real azure container.
		if p.ParentContainer != nil {
			blobPrefix = p.Name + "/" + blobPrefix
		} else {
			// parent IS nil, therefore we're in the real azure container.
			realContainer = p
		}

		p = p.ParentContainer

	}

	log.Debugf("Got container: %s , blobprefix: %s", realContainer.Name, blobPrefix)
	return realContainer, blobPrefix
}
