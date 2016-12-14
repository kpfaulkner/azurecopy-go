package utils

import (
	"azurecopy/azurecopy/handlers"
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/misc"
	"log"
)

// GetHandler gets the appropriate handler for the cloudtype.
// Should I be doing this another way?
func GetHandler(cloudType models.CloudType, isSource bool, config misc.CloudConfig, cacheToDisk bool) handlers.CloudHandlerInterface {
	switch cloudType {
	case models.Azure:

		accountName, accountKey := getAzureCredentials(isSource, config)

		log.Print("Got Azure Handler")
		ah, _ := handlers.NewAzureHandler(accountName, accountKey, isSource, cacheToDisk)
		return ah

	case models.Filesystem:
		log.Print("Got Filesystem Handler")
		fh, _ := handlers.NewFilesystemHandler("c:/temp/", isSource) // default path?
		return fh
	}

	return nil
}

func getAzureCredentials(isSource bool, config misc.CloudConfig) (accountName string, accountKey string) {
	if isSource {
		accountName = config.Credentials[misc.AzureSourceAccountName]
		accountKey = config.Credentials[misc.AzureSourceAccountKey]
	} else {
		accountName = config.Credentials[misc.AzureDestAccountName]
		accountKey = config.Credentials[misc.AzureDestAccountKey]
	}

	if accountName == "" || accountKey == "" {
		accountName = config.Credentials[misc.AzureDefaultAccountName]
		accountKey = config.Credentials[misc.AzureDefaultAccountKey]
	}

	return accountName, accountKey
}

// GetHandlerWithPathInfo gets the appropriate handler for the cloudtype.
// Should I be doing this another way?
func GetHandlerWithPathInfo(cloudType models.CloudType, useEmulator bool, cacheToDisk bool, pathInfo string) handlers.CloudHandlerInterface {

	/*
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
	*/
	return nil
}
