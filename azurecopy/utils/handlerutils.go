package utils

import (
	"azurecopy/azurecopy/handlers"
	"azurecopy/azurecopy/models"
)

// GetHandler gets the appropriate handler for the cloudtype.
// Should I be doing this another way?
func GetHandler(cloudType models.CloudType, useEmulator bool, cacheToDisk bool) handlers.CloudHandlerInterface {
	switch cloudType {
	case models.Azure:
		ah, _ := handlers.NewAzureHandler(useEmulator, cacheToDisk)
		return ah

	case models.Filesystem:
		fh := handlers.NewFilesystemHandler(useEmulator, cacheToDisk)
		return fh
	}

	return nil
}
