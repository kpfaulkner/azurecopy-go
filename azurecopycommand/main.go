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
		if c.Name == "img" {
			ac.GetContainerContents(c)

			blob, err := c.GetBlob("F9.JPG")
			if err != nil {
				log.Fatal(err)
			}
			azureRootContainer := ac.GetRootContainer(models.Azure)

			ac.ReadBlob(blob)

			tempContainer, err := azureRootContainer.GetContainer("temp")
			if err != nil {
				log.Fatal("GetContainer ", err)
			}

			ac.WriteBlob(tempContainer, blob)
		}
	}

}
