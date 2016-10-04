package main

import (
	"azurecopy/azurecopy"
	"azurecopy/azurecopy/models"
	"log"
)

// "so it begins"
func main() {

	ac := azurecopy.NewAzureCopy(true)

	rootContainer := ac.GetRootContainer(models.Filesystem)

	for _, c := range rootContainer.ContainerSlice {
		/*if c.Name == "temp" {
			ac.GetContainerContents(c)
			ac.ReadBlob(c.BlobSlice[0])
			c.DisplayContainer("")
		} */
		log.Println("Container ", c.Name)

	}

	for _, b := range rootContainer.BlobSlice {
		/*if c.Name == "temp" {
			ac.GetContainerContents(c)
			ac.ReadBlob(c.BlobSlice[0])
			c.DisplayContainer("")
		} */
		log.Println("Blob ", b.Name)

	}

}
