package containerutils

import "azurecopy/azurecopy/models"

// GetRootContainer Get root container of a simple container.
func GetRootContainer(container *models.SimpleContainer) *models.SimpleContainer {
	var p *models.SimpleContainer

	for p = container.ParentContainer; p.ParentContainer != nil; {
		p = p.ParentContainer
	}

	return p
}
