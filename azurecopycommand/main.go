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

func generateSpace(c int) string {
	s := ""
	for i := 0; i < c; i++ {
		s = s + " "
	}

	return s
}

func printContainer(container *models.SimpleContainer, depth int) {
	s := generateSpace(depth)

	log.Printf("%scontainer: %s", s, container.Name)

	depth = depth + 2
	s = generateSpace(depth)

	for _, b := range container.BlobSlice {
		log.Printf("%sblob: %s", s, b.Name)
	}

	for _, c := range container.ContainerSlice {
		printContainer(c, depth)
	}

}

// "so it begins"
func main() {

	// need to figure out how to read/parse args properly.
	//source := os.Args[1]
	//dest := os.Args[2]

	dest := "https://kenfau.blob.core.windows.net/temp/"
	source := "c:/temp/data/"

	fmt.Printf("Copying %s to %s", source, dest)

	config := misc.NewCloudConfig()

	ac := azurecopy.NewAzureCopy(source, dest, *config)
	//container, err := ac.ListContainer(source)
	//printContainer(container, 0)

	//if err != nil {
	//	log.Fatal(err)
	//}

	// http://127.0.0.1:10000/devaccount/devaccount/temp/
	err := ac.CopyBlobByURL(source, dest)
	if err != nil {
		log.Fatal(err)
	}

}
