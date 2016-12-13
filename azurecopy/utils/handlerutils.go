package utils

import (
	"azurecopy/azurecopy/handlers"
	"azurecopy/azurecopy/models"
	"log"
)

// GetHandler gets the appropriate handler for the cloudtype.
// Should I be doing this another way?
func GetHandler(cloudType models.CloudType, cacheToDisk bool) handlers.CloudHandlerInterface {
	switch cloudType {
	case models.Azure:

		log.Print("Got Azure Handler")
		ah, _ := handlers.NewAzureHandler(cacheToDisk)
		return ah

	case models.Filesystem:
		log.Print("Got Filesystem Handler")
		fh, _ := handlers.NewFilesystemHandler("c:/temp/") // default path?
		return fh
	}

	return nil
}

// GetHandler gets the appropriate handler for the cloudtype.
// Should I be doing this another way?
func GetHandlerWithPathInfo(cloudType models.CloudType, useEmulator bool, cacheToDisk bool, pathInfo string) handlers.CloudHandlerInterface {
	switch cloudType {
	case models.Azure:

		log.Print("Got Azure Handler")
		ah, _ := handlers.NewAzureHandler(useEmulator, cacheToDisk)
		return ah

	case models.Filesystem:
		log.Print("Got Filesystem Handler")
		fh, _ := handlers.NewFilesystemHandler(pathInfo)
		return fh
	}

	return nil
}
