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
			ac.GetContainerContents(c)

			c.DisplayContainer("")

			fmt.Println("Container children ", len(c.ContainerSlice))
			fmt.Println("Container2  children ", len(c.ContainerSlice[0].ContainerSlice))
		}
	}

}
