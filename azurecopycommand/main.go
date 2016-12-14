package main

import (
	"azurecopy/azurecopy"
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/misc"
	"fmt"
	"log"
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

func printContainer(container *models.SimpleContainer) {
	log.Printf("container %s", container.Name)
	for _, c := range container.ContainerSlice {
		log.Printf("container: %s", c.Name)
		printContainer(c)
	}

	for _, b := range container.BlobSlice {
		log.Printf("blob: %s", b.Name)
	}
}

// "so it begins"
func main() {

	// need to figure out how to read/parse args properly.
	//source := os.Args[1]
	//dest := os.Args[2]

	source := "https://kenfau.blob.core.windows.net/temp/"
	dest := ""

	fmt.Printf("Copying %s to %s", source, dest)

	config := misc.NewCloudConfig()

	config.Credentials[misc.AzureSourceAccountName] = "kenfau"
	config.Credentials[misc.AzureSourceAccountKey] = "lFXm0+/xwZK3Cg8Bd/lCXnH5KwgRYFN3VPpxtPyxFXjG6csS5CAh1peudp5h5nh15PQwA3vPwsOjkR/54d6X1w=="

	ac := azurecopy.NewAzureCopy(source, dest, *config)
	container, err := ac.ListContainer(source)
	printContainer(container)

	if err != nil {
		log.Fatal(err)
	}

	// http://127.0.0.1:10000/devaccount/devaccount/temp/
	//err := ac.CopyBlobByURL(source, dest)

	log.Println(err)

}
