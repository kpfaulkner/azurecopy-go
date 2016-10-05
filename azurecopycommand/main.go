package main

import (
	"azurecopy/azurecopy"
	"azurecopy/azurecopy/models"
)

// "so it begins"
func main() {

	ac := azurecopy.NewAzureCopy(true)

	rootContainer := ac.GetRootContainer(models.Filesystem)

	for _, c := range rootContainer.ContainerSlice {
		if c.Name == "autorest" {
			ac.GetContainerContents(c)
			ac.ReadBlob(c.BlobSlice[0])
			c.DisplayContainer("")
		}
	}

}
