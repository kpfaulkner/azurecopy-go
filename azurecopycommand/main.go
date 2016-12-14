package main

import (
	"azurecopy/azurecopy"
	"azurecopy/azurecopy/utils/misc"
	"fmt"
	"log"
	"os"
)

/*
func FSToAzure() {
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

func messingABout() {
	ac := azurecopy.NewAzureCopy(true)

	azureRootContainer := ac.GetRootContainer(models.Azure)

	for _, c := range azureRootContainer.ContainerSlice {
		if c.Name == "temp" {
			ac.GetContainerContents(c)

			blob, err := c.GetBlob("F9.JPG")
			if err != nil {
				log.Fatal(err)
			}
			rootContainer := ac.GetRootContainer(models.Filesystem)

			ac.ReadBlob(blob)

			tempContainer, err := rootContainer.GetContainer("temp")
			if err != nil {
				log.Fatal("GetContainer ", err)
			}

			ac.WriteBlob(tempContainer, blob)
		}
	}

}
*/

// "so it begins"
func main() {

	// need to figure out how to read/parse args properly.
	source := os.Args[1]
	dest := os.Args[2]

	fmt.Printf("Copying %s to %s", source, dest)

	config := misc.NewCloudConfig()
	ac := azurecopy.NewAzureCopy(source, dest, *config)

	err := ac.CopyBlobByURL("http://127.0.0.1:10000/devaccount/devaccount/temp/", "c:/temp/temp/")

	log.Println(err)

}
