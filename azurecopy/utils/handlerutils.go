package utils

import (
	"azurecopy/azurecopy/handlers"
	"azurecopy/azurecopy/models"
)

// GetHandler gets the appropriate handler for the cloudtype.
// Should I be doing this another way?
func GetHandler(cloudType models.CloudType, useEmulator bool) handlers.CloudHandlerInterface {
	switch cloudType {
	case models.Azure:
		ah := handlers.NewAzureHandler(useEmulator)
		return ah

	case models.Filesystem:
		fh := handlers.NewFilesystemHandler(useEmulator)
		return fh
	}

	return nil
}
