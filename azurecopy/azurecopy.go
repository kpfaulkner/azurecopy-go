package azurecopy

import (
	"azurecopy/azurecopy/handlers"
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils"
	"azurecopy/azurecopy/utils/misc"
	"log"
	"regexp"
	"strings"
)

// AzureCopy main client class.
// Have one instance of this PER cloud env.
// ie one for source and one for destination.
type AzureCopy struct {
	config misc.CloudConfig

	// source/destination URLS
	sourceURL string
	destURL   string

	// cloud types
	sourceCloudType models.CloudType
	destCloudType   models.CloudType

	// handlers
	sourceHandler handlers.CloudHandlerInterface
	destHandler   handlers.CloudHandlerInterface
}

// NewAzureCopy factory time!
// want to know source/dest up front.
func NewAzureCopy(sourceURL string, destURL string, config misc.CloudConfig) *AzureCopy {
	ac := AzureCopy{}
	ac.config = config
	ac.destURL = destURL
	ac.sourceURL = sourceURL

	ac.sourceCloudType, _ = ac.getCloudType(ac.sourceURL)
	ac.destCloudType, _ = ac.getCloudType(ac.destURL)

	ac.sourceHandler = ac.GetHandlerForURL(ac.sourceURL, true, true)
	ac.destHandler = ac.GetHandlerForURL(ac.destURL, false, true)

	return &ac
}

// Get Cloud Type...
// Should pre-compile all of these regexs
func (ac *AzureCopy) getCloudType(url string) (cloudType models.CloudType, isEmulator bool) {
	lowerURL := strings.ToLower(url)

	// Azure
	match, _ := regexp.MatchString("blob.core.windows.net", lowerURL)
	if match {
		return models.Azure, false
	}

	// Azure emulator
	match, _ = regexp.MatchString("127.0.0.1:10000", lowerURL)
	if match {
		return models.Azure, true
	}

	// S3
	// need to think about S3 compatible devices. TODO(kpfaulkner)
	match, _ = regexp.MatchString("s3.amazonaws.com", lowerURL)
	if match {
		return models.S3, false
	}

	return models.Filesystem, false
}

// ListContainer lists containers/blobs in URL
func (ac *AzureCopy) ListContainer(sourceURL string) (*models.SimpleContainer, error) {
	log.Printf("Listing contents of %s", sourceURL)

	container, err := ac.sourceHandler.GetSpecificSimpleContainer(sourceURL)
	if err != nil {
		log.Fatal("ListContainer failed ", err)
	}

	// get the blobs for the deepest vdir which is part of the URL.
	ac.sourceHandler.GetContainerContents(container)

	return container, nil
}

// CopyBlobByURL copy a blob from one URL to another.
// Format of sourceURL and destURL are REAL URLS.
// No dodgy made up prefix.
// TODO(kpfaulkner) need to figure out cache and emulator params here.
func (ac *AzureCopy) CopyBlobByURL(sourceURL string, destURL string) error {

	var err error
	if misc.GetLastChar(sourceURL) == "/" {
		// copying a directory/vdir worth of stuff....
		err = ac.CopyContainerByURL(sourceURL, destURL)
	} else {
		err = ac.CopySingleBlobByURL(sourceURL, destURL)
	}

	if err != nil {
		log.Fatal("CopyBlobByUrl error ", err)
	}
	return nil
}

// CopySingleBlobByURL copies a single blob referenced by URL to a destination URL
func (ac *AzureCopy) CopySingleBlobByURL(sourceURL string, destURL string) error {

	deepestContainer, err := ac.sourceHandler.GetSpecificSimpleContainer(sourceURL)
	if err != nil {
		return err
	}

	// get the blobs for the deepest vdir which is part of the URL.
	ac.sourceHandler.GetContainerContents(deepestContainer)

	return nil
}

// CopyContainerByURL copies blobs/containers from a URL to a destination URL.
func (ac *AzureCopy) CopyContainerByURL(sourceURL string, destURL string) error {

	log.Printf("copy from %s to %s", sourceURL, destURL)

	deepestContainer, err := ac.sourceHandler.GetSpecificSimpleContainer(sourceURL)
	if err != nil {
		log.Fatal("CopyContainerByURL failed ", err)
	}

	// get the blobs for the deepest vdir which is part of the URL.
	ac.sourceHandler.GetContainerContents(deepestContainer)

	deepestDestinationContainer, err := ac.destHandler.GetSpecificSimpleContainer(destURL)
	if err != nil {
		log.Fatal("CopyContainerByURL failed ", err)
	}

	// recursive...  dangerous...
	ac.copyAllBlobsInContainer(deepestContainer, deepestDestinationContainer, "")
	// write out all the blobs.... loop through BlobSlice, ContainerSlice and all sub blobs.
	//ac.WriteBlob(deepestDestinationContainer, blob)

	return nil
}

// copyAllBlobsInContainer recursively copies all blobs (in sub containers) to the destination.
func (ac *AzureCopy) copyAllBlobsInContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer, prefix string) error {

	// copy all blobs
	// nothing concurrent YET
	for _, blob := range sourceContainer.BlobSlice {
		ac.ReadBlob(blob)
		origName := blob.Name

		if prefix != "" {
			blob.Name = prefix + "/" + blob.Name
		}

		log.Printf("Read %s and writing as %s", origName, blob.Name)

		// modify blob name?
		// hacky? Options? TODO(kpfaulkner)
		ac.WriteBlob(destContainer, blob)
	}

	// call for each sub container.
	for _, container := range sourceContainer.ContainerSlice {
		var newPrefix string
		if prefix != "" {
			newPrefix = prefix + "/" + container.Name
		} else {
			newPrefix = container.Name
		}

		err := ac.copyAllBlobsInContainer(container, destContainer, newPrefix)
		if err != nil {
			log.Fatal(err)
		}
	}

	return nil
}

// GetHandlerForURL returns the appropriate handler for a given cloud type.
func (ac *AzureCopy) GetHandlerForURL(url string, isSource bool, cacheToDisk bool) handlers.CloudHandlerInterface {
	cloudType, _ := ac.getCloudType(url)
	handler := utils.GetHandler(cloudType, isSource, ac.config, cacheToDisk)
	return handler
}

func (ac *AzureCopy) GetSourceRootContainer() models.SimpleContainer {
	rootContainer := ac.sourceHandler.GetRootContainer()
	return rootContainer
}

func (ac *AzureCopy) GetDestRootContainer() models.SimpleContainer {
	rootContainer := ac.destHandler.GetRootContainer()
	return rootContainer
}

// GetContainerContents populates the container with data.
func (ac *AzureCopy) GetContainerContents(container *models.SimpleContainer) {

	// check where container came from.
	if container.IsSource {
		ac.sourceHandler.GetContainerContents(container)
	} else {
		ac.destHandler.GetContainerContents(container)
	}
}

// GetDestContainerContents populates the container with data.
func (ac *AzureCopy) GetDestContainerContents(container *models.SimpleContainer) {
	ac.destHandler.GetContainerContents(container)
}

// ReadBlob reads a blob and keeps it in memory OR caches to disk.
// (or in the special case of azure copyblob flag it will do something tricky, once I get to that part)
func (ac *AzureCopy) ReadBlob(blob *models.SimpleBlob) {

	log.Println("ReadBlob " + blob.Name)
	err := ac.sourceHandler.PopulateBlob(blob)

	if err != nil {
		log.Fatal(err)

	}
}

// WriteBlob writes a source blob (can be from anywhere) to a destination container (can and probably will be a different cloud platform)
func (ac *AzureCopy) WriteBlob(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {

	if destContainer == nil {
		log.Print("dest container is nil")
	} else {
		log.Printf("write dest loc %s ", destContainer.URL)
	}

	if err := ac.destHandler.WriteBlob(destContainer, sourceBlob); err != nil {
		log.Fatal("WriteBlob kaboom ", err)
	}
	return nil
}
