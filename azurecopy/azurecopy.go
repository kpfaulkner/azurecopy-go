package azurecopy

import (
	"azurecopy/azurecopy/handlers"
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils"
	"azurecopy/azurecopy/utils/misc"
	"fmt"
	"os"
	"regexp"
	"strings"

	"sync"

	log "github.com/Sirupsen/logrus"
)

var wg sync.WaitGroup

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

	/*
		// make channel
		readChannel := make(chan models.SimpleContainer, 1000)

		// get container contents over channel.
		// get the blobs for the deepest vdir which is part of the URL.
		go ac.sourceHandler.GetContainerContentsOverChannel(*container, readChannel)
		if err != nil {
			log.Fatalf("ListContainer err %s", err)
		}

		log.Debug("about to loop")

		for {
			// get data read.
			containerDetails, ok := <-readChannel
			if !ok {
				log.Debug("breaking, read channel closed")

				// channel closed. We're done now.
				break
			}
			// merge containerDetails into original container
			ac.mergeContainerDetails(&containerDetails, container)
		}
	*/
	return container, nil
}

// CreateContainer lists containers/blobs in URL
func (ac *AzureCopy) CreateContainer(containerName string) error {
	log.Debugf("CreateContainer %s", containerName)

	_, err := ac.sourceHandler.CreateContainer(containerName)
	if err != nil {
		log.Fatal("CreateContainer failed ", err)
	}

	return nil
}

// CopyBlobByURLUsingCopyBlob copy a blob from one URL to another using Azure CopyBlob functionality.
// This only works if the destination is Azure.
func (ac *AzureCopy) CopyBlobByURLUsingCopyBlob(replaceExisting bool) error {

	// check destination is Azure. If not, kaboom!
	if ac.destCloudType != models.Azure {
		fmt.Printf("Can only use CopyBlob flag when destination is Azure Blob Storage")
		os.Exit(1)
	}

	var err error

	// need to make this cloud/fs agnostic!
	if misc.GetLastChar(ac.sourceURL) == "/" || misc.GetLastChar(ac.sourceURL) == "\\" {
		// copying a directory/vdir worth of stuff....
		err = ac.CopyContainerByURLUsingCopyBlob(ac.sourceURL, ac.destURL, replaceExisting)
	} else {
		err = ac.CopySingleBlobByURL(ac.sourceURL, ac.destURL, replaceExisting)
	}

	if err != nil {
		log.Fatal("CopyBlobByUrl error ", err)
	}
	return nil
}

// CopyBlobByURL copy a blob from one URL to another.
func (ac *AzureCopy) CopyBlobByURL(replaceExisting bool) error {

	var err error
	if misc.GetLastChar(ac.sourceURL) == "/" || misc.GetLastChar(ac.sourceURL) == "\\" {
		// copying a directory/vdir worth of stuff....
		err = ac.CopyContainerByURL(ac.sourceURL, ac.destURL, replaceExisting)
	} else {
		err = ac.CopySingleBlobByURL(ac.sourceURL, ac.destURL, replaceExisting)
	}

	if err != nil {
		log.Fatal("CopyBlobByUrl error ", err)
	}
	return nil
}

// CopySingleBlobByURL copies a single blob referenced by URL to a destination URL
func (ac *AzureCopy) CopySingleBlobByURL(sourceURL string, destURL string, replaceExisting bool) error {

	deepestContainer, err := ac.sourceHandler.GetSpecificSimpleContainer(sourceURL)
	if err != nil {
		return err
	}

	// get the blobs for the deepest vdir which is part of the URL.
	ac.sourceHandler.GetContainerContents(deepestContainer)

	return nil
}

// CopyContainerByURL copies blobs/containers from a URL to a destination URL.
// This CURRENTLY uses a different method of generating blobs and containers.
// The plan is to consolidate both listing and copying into using the same methods, but for now
// want to make sure copying at least is able to start copying blobs before the listing is finished.
// So will use GoRoutines to concurrently retrieve list of blobs and another for writing to destination.
// First real attempt using GoRoutines for something "real" so will see how it goes.
func (ac *AzureCopy) CopyContainerByURL(sourceURL string, destURL string, replaceExisting bool) error {

	log.Infof("copy from %s to %s", sourceURL, destURL)

	deepestContainer, err := ac.sourceHandler.GetSpecificSimpleContainer(sourceURL)
	if err != nil {
		log.Fatal("CopyContainerByURL failed source ", err)
	}

	deepestDestinationContainer, err := ac.destHandler.GetSpecificSimpleContainer(destURL)
	if err != nil {
		log.Fatal("CopyContainerByURL failed dest ", err)
	}

	log.Debug("about to get over channel")
	deepestDestinationContainer.DisplayContainer("")

	// make channel
	readChannel := make(chan models.SimpleContainer, 1000)

	// get container contents over channel.
	// get the blobs for the deepest vdir which is part of the URL.
	go ac.sourceHandler.GetContainerContentsOverChannel(*deepestContainer, readChannel)
	if err != nil {
		log.Fatalf("CopyContainerByURL err %s", err)
	}

	log.Debug("about to loop")

	for {
		// get data read.
		containerDetails, ok := <-readChannel
		if !ok {
			log.Debug("breaking, read channel closed")

			// channel closed. We're done now.
			break
		}

		log.Debugf("containerDetails reading from channel")
		containerDetails.DisplayContainer("")

		// copy it.
		// recursive...  dangerous...
		wg.Add(1)
		go ac.copyAllBlobsInContainer(&containerDetails, deepestDestinationContainer, "", replaceExisting)
	}

	wg.Wait()

	log.Debug("after wait")
	return nil
}

// CopyContainerByURLUsingCopyBlob copies blobs/containers from a URL to a destination Azure URL
// using the CopyBlob functionality within Azure.
// This means that the local bandwidth is not required and the files get copied directly from
// the source to Azure.
func (ac *AzureCopy) CopyContainerByURLUsingCopyBlob(sourceURL string, destURL string, replaceExisting bool) error {

	log.Infof("copy from %s to %s", sourceURL, destURL)

	deepestContainer, err := ac.sourceHandler.GetSpecificSimpleContainer(sourceURL)
	if err != nil {
		log.Fatal("CopyContainerByURL failed source ", err)
	}

	deepestDestinationContainer, err := ac.destHandler.GetSpecificSimpleContainer(destURL)
	if err != nil {
		log.Fatal("CopyContainerByURL failed dest ", err)
	}

	log.Debug("about to get over channel")
	deepestDestinationContainer.DisplayContainer("")

	// make channel
	readChannel := make(chan models.SimpleContainer, 1000)

	// get container contents over channel.
	// get the blobs for the deepest vdir which is part of the URL.
	go ac.sourceHandler.GetContainerContentsOverChannel(*deepestContainer, readChannel)
	if err != nil {
		log.Fatalf("CopyContainerByURL err %s", err)
	}

	log.Debug("about to loop")

	for {
		// get data read.
		containerDetails, ok := <-readChannel
		if !ok {
			log.Debug("breaking, read channel closed")

			// channel closed. We're done now.
			break
		}

		log.Debugf("containerDetails reading from channel")
		containerDetails.DisplayContainer("")

		// copy it.
		// recursive...  dangerous...
		wg.Add(1)
		go ac.copyAllBlobsInContainer(&containerDetails, deepestDestinationContainer, "", replaceExisting)
	}

	wg.Wait()

	log.Debug("after wait")
	return nil
}

// copyAllBlobsInContainer recursively copies all blobs (in sub containers) to the destination.
// Have wrapper function to implementCopyAllBlobsInContainer since we have recursive calls and cant have recursive wg.Done's
func (ac *AzureCopy) copyAllBlobsInContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer, prefix string, replaceExisting bool) error {

	log.Debug("copyAllBlobsInContainer start")
	defer wg.Done()
	return ac.implementCopyAllBlobsInContainer(sourceContainer, destContainer, prefix, replaceExisting)

}

// implementCopyAllBlobsInContainer recursively copies all blobs (in sub containers) to the destination.
func (ac *AzureCopy) implementCopyAllBlobsInContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer, prefix string, replaceExisting bool) error {

	log.Debug("implementCopyAllBlobsInContainer start")

	// copy all blobs
	for _, blob := range sourceContainer.BlobSlice {

		log.Debugf("blob is %s", blob)

		// check if blob exists if we're not replacing
		if !replaceExisting {
			exists, err := ac.doesDestinationBlobExist(destContainer, blob)
			if err != nil {
				log.Fatalf("Error", err)
			}

			// exists and we dont want to replace
			if exists {
				continue
			}
		}

		ac.ReadBlob(blob)
		origName := blob.Name

		if prefix != "" {
			blob.Name = prefix + "/" + blob.Name
		}

		log.Debugf("Read %s and writing as %s", origName, blob.Name)

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

		err := ac.implementCopyAllBlobsInContainer(container, destContainer, newPrefix, replaceExisting)
		if err != nil {
			log.Fatal(err)
		}
	}

	return nil
}

// GetHandlerForURL returns the appropriate handler for a given cloud type.
func (ac *AzureCopy) GetHandlerForURL(url string, isSource bool, cacheToDisk bool) handlers.CloudHandlerInterface {
	log.Debugf("GetHandlerForURL %s", url)
	cloudType, isEmulator := ac.getCloudType(url)
	handler := utils.GetHandler(cloudType, isSource, ac.config, cacheToDisk, isEmulator)
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

// doesDestinationBlobExist checks if the destination blob exists
func (ac *AzureCopy) doesDestinationBlobExist(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) (bool, error) {

	if destContainer == nil {
		log.Debugf("dest container is nil")
	} else {
		log.Debugf("write dest loc %s ", destContainer.URL)
	}

	if err := ac.destHandler.WriteBlob(destContainer, sourceBlob); err != nil {
		log.Fatal("WriteBlob kaboom ", err)
	}
	return false, nil
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
