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

			// now get subdir.
			subDir := c.ContainerSlice[0]
			ac.GetContainerContents(subDir)

			c.DisplayContainer("")
		}
	}

}
