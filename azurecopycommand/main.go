package main

import (
	"azurecopy/azurecopy"
	"azurecopy/azurecopy/models"
	"fmt"
)

// "so it begins"
func main() {

	ac := azurecopy.AzureCopy{}

	rootContainer = ac.GetRootContainer(models.Azure, true)

	fmt.Println(rootContainer.ContainerSlice[0].Name)

	// get a subcontainer.
	//sc := ah.GetContainer(rootContainer.ContainerSlice[0].Name)

	fmt.Println(sc.BlobSlice[0].Name)
}
