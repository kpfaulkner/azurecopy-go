package azurecopy

import (
	"azurecopy/azurecopy/handlers"
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils"
	"log"
	"strings"
)

// AzureCopy main client class.
type AzureCopy struct {

	// list of blobs.
	BlobSlice []models.SimpleBlob

	// list of containers.
	ContainerSlice []models.SimpleContainer

	// are we using an emualtor?
	UseEmulator bool
}

// NewAzureCopy factory time!
func NewAzureCopy(useEmulator bool) *AzureCopy {
	ac := AzureCopy{}

	ac.BlobSlice = []models.SimpleBlob{}
	ac.ContainerSlice = []models.SimpleContainer{}
	ac.UseEmulator = useEmulator

	return &ac
}

func (ac *AzureCopy) getCloudType(url string) models.CloudType {
	lowerURL := strings.ToLower(url)

	if lowerURL[0:6] == "azure" {
		return models.Azure
	}
	return models.Filesystem
}

// CopyBlobByUrl copy a blob from one URL to another.
// TODO(kpfaulkner) need to figure out cache and emulator params here.
func (ac *AzureCopy) CopyBlobByUrl(sourceURL string, destURL string) error {

	ah, err := handlers.NewAzureHandler(true, true)
	if err != nil {
		log.Fatal(err)
	}

	data, err := ah.GetSpecificSimpleContainer(sourceURL)

	log.Println(data, err)

	/*
		sourceHandler := ac.GetHandlerForURL(sourceURL, true, true)
		destHandler := ac.GetHandlerForURL(destURL, true, true)

		sourceBlob, err := sourceHandler.ReadDirectBlob(sourceURL)
		if err != nil {
			log.Panic("CopyBlobByUrl failed %s to %s", sourceURL, destURL)
		}

		err = destHandler.WriteDirectBlob(sourceBlob, destURL)
		if err != nil {
			log.Fatal("Writing blob to %s failed", destURL)
		}*/

	return nil
}

// GetHandlerForURL returns the appropriate handler for a given cloud type.
func (ac *AzureCopy) GetHandlerForURL(url string, useEmulator bool, cacheToDisk bool) handlers.CloudHandlerInterface {
	cloudType := ac.getCloudType(url)
	handler := utils.GetHandler(cloudType, useEmulator, cacheToDisk)
	return handler
}

// GetRootContainer get the root container (and immediate containers/blobs)
func (ac *AzureCopy) GetRootContainer(cloudType models.CloudType) models.SimpleContainer {

	handler := utils.GetHandler(cloudType, ac.UseEmulator, true)
	rootContainer := handler.GetRootContainer()
	return rootContainer
}

// GetContainerContents populates the container with data.
func (ac *AzureCopy) GetContainerContents(container *models.SimpleContainer) {
	handler := utils.GetHandler(container.Origin, ac.UseEmulator, true)
	handler.GetContainerContents(container, ac.UseEmulator)
}

// ReadBlob reads a blob and keeps it in memory OR caches to disk.
// (or in the special case of azure copyblob flag it will do something tricky, once I get to that part)
func (ac *AzureCopy) ReadBlob(blob *models.SimpleBlob) {

	log.Println("ReadBlob " + blob.Name)
	handler := utils.GetHandler(blob.Origin, ac.UseEmulator, true)
	err := handler.PopulateBlob(blob)

	if err != nil {
		log.Fatal(err)

	}
}

// WriteBlob writes a source blob (can be from anywhere) to a destination container (can and probably will be a different cloud platform)
func (ac *AzureCopy) WriteBlob(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {
	handler := utils.GetHandler(destContainer.Origin, ac.UseEmulator, true)

	if err := handler.WriteBlob(destContainer, sourceBlob); err != nil {
		log.Fatal("WriteBlob kaboom ", err)
	}
	return nil
}
