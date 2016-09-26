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

	firstContainer := rootContainer.ContainerSlice[0]

	// populate subcontainer.
	ac.GetContainerContents(&firstContainer)

	//fmt.Println(firstContainer.BlobSlice[0].Name)
}
