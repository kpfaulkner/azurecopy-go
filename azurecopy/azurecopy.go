package azurecopy

import (
	"azurecopy/azurecopy/handlers"
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils"
	"azurecopy/azurecopy/utils/misc"
	"regexp"
	"strings"

	"sync"

	log "github.com/Sirupsen/logrus"
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
func NewAzureCopy(config misc.CloudConfig) *AzureCopy {
	ac := AzureCopy{}
	ac.config = config

	// technically duped from config, but just easier to reference.
	ac.destURL = config.Configuration[misc.Dest]
	ac.sourceURL = config.Configuration[misc.Source]

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
func (ac *AzureCopy) ListContainer() (*models.SimpleContainer, error) {
	log.Debugf("Listing contents of %s", ac.sourceURL)

	container, err := ac.sourceHandler.GetSpecificSimpleContainer(ac.sourceURL)
	if err != nil {
		log.Fatal("ListContainer failed ", err)
	}

	// get the blobs for the deepest vdir which is part of the URL.
	ac.sourceHandler.GetContainerContents(container)

	return container, nil
}

// CopyBlobByURL copy a blob from one URL to another.
func (ac *AzureCopy) CopyBlobByURL() error {

	var err error
	if misc.GetLastChar(ac.sourceURL) == "/" {
		// copying a directory/vdir worth of stuff....
		err = ac.CopyContainerByURL(ac.sourceURL, ac.destURL)
	} else {
		err = ac.CopySingleBlobByURL(ac.sourceURL, ac.destURL)
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

	log.Infof("copy from %s to %s", sourceURL, destURL)

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

	return nil
}

// copyAllBlobsInContainer recursively copies all blobs (in sub containers) to the destination.
func (ac *AzureCopy) copyAllBlobsInContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer, prefix string) error {

	var wg sync.WaitGroup
	wg.Add(len(sourceContainer.BlobSlice))

	// copy all blobs
	for _, blob := range sourceContainer.BlobSlice {

		go func() {
			defer wg.Done()
			ac.ReadBlob(blob)
			origName := blob.Name

			if prefix != "" {
				blob.Name = prefix + "/" + blob.Name
			}

			log.Debugf("Read %s and writing as %s", origName, blob.Name)

			// modify blob name?
			// hacky? Options? TODO(kpfaulkner)
			ac.WriteBlob(destContainer, blob)
		}()
	}
	wg.Wait()

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
	log.Debugf("GetHandlerForURL %s", url)
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

	log.Debugf("ReadBlob %s", blob.Name)
	err := ac.sourceHandler.PopulateBlob(blob)

	if err != nil {
		log.Fatal(err)

	}
}

// WriteBlob writes a source blob (can be from anywhere) to a destination container (can and probably will be a different cloud platform)
func (ac *AzureCopy) WriteBlob(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {

	if destContainer == nil {
		log.Debugf("dest container is nil")
	} else {
		log.Debugf("write dest loc %s ", destContainer.URL)
	}

	if err := ac.destHandler.WriteBlob(destContainer, sourceBlob); err != nil {
		log.Fatal("WriteBlob kaboom ", err)
	}
	return nil
}
