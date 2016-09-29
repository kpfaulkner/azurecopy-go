package main

import (
	"azurecopy/azurecopy"
	"azurecopy/azurecopy/models"
)

// "so it begins"
func main() {

	ac := azurecopy.NewAzureCopy(true)

	rootContainer := ac.GetRootContainer(models.Azure)

	for _, c := range rootContainer.ContainerSlice {
		if c.Name == "temp" {
			ac.GetContainerContents(c)
			c.DisplayContainer("")
		}
	}

}
