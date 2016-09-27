package main

import (
	"azurecopy/azurecopy"
	"azurecopy/azurecopy/models"
	"fmt"
)

// "so it begins"
func main() {

	ac := azurecopy.NewAzureCopy(true)

	rootContainer := ac.GetRootContainer(models.Azure)

	fmt.Println(rootContainer.ContainerSlice[0].Name)

	for _, c := range rootContainer.ContainerSlice {
		if c.Name == "temp" {
			ac.GetContainerContents(&c)

			c.DisplayContainer("")
		}
	}

}
