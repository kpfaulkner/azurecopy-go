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

// misc consts for credentials.
// need a more dynamic way to add for new cloud types.
// but for now, it will do.
const (
	// Azure.
	AzureDefaultAccountName = "AzureDefaultAccountName"
	AzureDefaultAccountKey  = "AzureDefaultAccountKey"
	AzureSourceAccountName  = "AzureSourceAccountName"
	AzureSourceAccountKey   = "AzureSourceAccountKey"
	AzureDestAccountName    = "AzureDestAccountName"
	AzureDestAccountKey     = "AzureDestAccountKey"

	// S3
	S3DefaultAccessID     = "S3DefaultAccessID"
	S3DefaultAccessSecret = "S3DefaultAccessSecret"
	S3SourceAccessID      = "S3SourceAccessID"
	S3SourceAccessSecret  = "S3SourceAccessSecret"
	S3DestAccessID        = "S3DestAccessID"
	S3DestAccessSecret    = "S3DestAccessSecret"
)

// AzureCopy main client class.
// Have one instance of this PER cloud env.
// ie one for source and one for destination.
type AzureCopy struct {

	// credentials used for various handlers.
	// main application will pass these in.
	credentials map[string]string

	// source/destination URLS
	sourceURL string
	destURL   string

	// cloud types
	sourceCloudType models.CloudType
	destCloudType   models.CloudType

	// handlers
	sourceHandler handlers.CloudHandlerInterface
	destHandler handlers.CloudHandlerInterface
}

// NewAzureCopy factory time!
// want to know source/dest up front.
func NewAzureCopy(sourceURL string, destURL string) *AzureCopy {
	ac := AzureCopy{}
	ac.destURL = destURL
	ac.sourceURL = sourceURL

	ac.sourceCloudType = ac.getCloudType(ac.sourceURL)
	ac.destCloudType = ac.getCloudType(ac.destURL)

	ac.sourceHandler := ac.GetHandlerForURL(ac.sourceURL, true)
	ac.destHandler := ac.GetHandlerForURL(ac.destURL, true)

	ac.credentials = make(map[string]string)
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
	match, _ = regexp.MatchString("amazons3.com", lowerURL)
	if match {
		return models.S3, false
	}

	return models.Filesystem, false
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

	ah, err := handlers.NewAzureHandler(true, true)
	if err != nil {
		log.Fatal(err)
	}

	deepestContainer, err := ah.GetSpecificSimpleContainer(sourceURL)

	// get the blobs for the deepest vdir which is part of the URL.
	ah.GetContainerContents(deepestContainer, true)

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
	sourceHandler.GetContainerContents(deepestContainer, true)

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
		err := ac.copyAllBlobsInContainer(container, destContainer, prefix+"/"+sourceContainer.Name)
		if err != nil {
			log.Fatal(err)
		}
	}

	return nil
}

// GetHandlerForURL returns the appropriate handler for a given cloud type.
func (ac *AzureCopy) GetHandlerForURL(url string, cacheToDisk bool) handlers.CloudHandlerInterface {
	cloudType, isEmulator := ac.getCloudType(url)
	handler := utils.GetHandler(cloudType, isEmulator, cacheToDisk)
	return handler
}

func (ac *AzureCopy) GetSourceRootContainer() models.SimpleContainer {
	rootContainer := ac.sourceHandler.GetRootContainer();
	return rootContainer
}

func (ac *AzureCopy) GetDestRootContainer() models.SimpleContainer {
	rootContainer := ac.destHandler.GetRootContainer();
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
	ac.destHandler.GetContainerContents(container, ac.UseEmulator)
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

	if destContainer == nil {
		log.Print("dest container is nil")
	} else {
		log.Printf("write dest loc %s ", destContainer.URL)
	}

	handler := utils.GetHandler(destContainer.Origin, ac.UseEmulator, true)

	log.Print("WriteBlob have handler ", handler)

	if err := handler.WriteBlob(destContainer, sourceBlob); err != nil {
		log.Fatal("WriteBlob kaboom ", err)
	}
	return nil
}
