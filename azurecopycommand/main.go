package main

import (
	"azurecopy/azurecopy"
	"azurecopy/azurecopy/models"
)

// "so it begins"
func main() {

	ac := azurecopy.AzureCopy{}

	ac.GetHandler(models.Azure)

}
