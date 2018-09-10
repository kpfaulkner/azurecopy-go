package handlers

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/containerutils"
	"azurecopy/azurecopy/utils/misc"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/satori/uuid"

	storage "github.com/azure/azure-storage-blob-go/2016-05-31/azblob"
	"time"
	"context"

	"bytes"
)

type AzureHandler struct {
	serviceURL storage.ServiceURL

	// determine if we're caching the blob to disk during copy operations.
	// or if we're keeping it in memory
	cacheToDisk   bool
	cacheLocation string

	// is this handler for the source or dest?
	IsSource bool

	// dealing with emulator.
	IsEmulator bool
}

// NewAzureHandler factory to create new one. Evil?
func NewAzureHandler(accountName string, accountKey string, isSource bool, cacheToDisk bool, isEmulator bool) (*AzureHandler, error) {
	ah := new(AzureHandler)

	ah.cacheToDisk = cacheToDisk
	dir, err := ioutil.TempDir("", "azurecopy")
	if err != nil {
		log.Fatalf("Unable to create temp directory %s", err)
	}

	ah.cacheLocation = dir
	ah.IsSource = isSource
	ah.IsEmulator = isEmulator

	if isEmulator || (accountName == "" && accountKey == "") {
		// set emulator accountName and accountKey
	}

	credential := storage.NewSharedKeyCredential(accountName, accountKey)
	p := storage.NewPipeline(credential, storage.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	serviceURL := storage.NewServiceURL(*u, p)

	if err != nil {

		// indicate error somehow..  still trying to figure that out with GO.
		return nil, err
	}

	ah.serviceURL = serviceURL
	return ah, nil
}

// DoCopyBlobUsingAzureCopyBlobFlag copy using Azure CopyBlob flag.
func (ah *AzureHandler) DoCopyBlobUsingAzureCopyBlobFlag(sourceBlob *models.SimpleBlob, destContainer *models.SimpleContainer, destBlobName string) error {

	return nil
}

// GetRootContainer gets root container of Azure. In reality there isn't a root container, but this would basically be a SimpleContainer
// that has the containerSlice populated with the real Azure containers.
func (ah *AzureHandler) GetRootContainer() models.SimpleContainer {

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	containerResponse, _:= ah.serviceURL.ListContainers( ctx, storage.Marker{}, storage.ListContainersOptions{}  )
	rootContainer := models.NewSimpleContainer()

	for _, c := range containerResponse.Containers {
		sc := models.NewSimpleContainer()
		sc.Name = c.Name
		sc.Origin = models.Azure

		rootContainer.ContainerSlice = append(rootContainer.ContainerSlice, sc)
	}

	return *rootContainer
}

// BlobExists checks if blob exists
func (ah *AzureHandler) BlobExists(container models.SimpleContainer, blobName string) (bool, error) {

	azureContainerName, _ := ah.getContainerAndBlobNames(&container, blobName)
	containerURL := ah.serviceURL.NewContainerURL(azureContainerName)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	// must be a better way surely?
	resp, err := containerURL.ListBlobs(ctx, storage.Marker{}, storage.ListBlobsOptions{Prefix: blobName})
	if err != nil {
		return false, err
	}

	for _,bn := range resp.Blobs.Blob {
		if bn.Name == blobName {
			return true, nil
		}
	}

	return false, nil
}

// GetSpecificSimpleContainer given a URL (ending in /) then get the SIMPLE container that represents it.
// returns the container of the last most part of the url.
// eg. if the url was https://myacct.blob.core.windows.net/realazurecontainer/vdir1/vdir2/  then the simple container
// returned is vdir2.
func (ah *AzureHandler) GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error) {

	lastChar := URL[len(URL)-1:]
	// MUST be a better way to get the last character.
	if lastChar != "/" {
		return nil, errors.New("Needs to end with a /")
	}

	_, containerName, blobPrefix, _, err := ah.validateURL(URL)
	if err != nil {
		log.Fatal("GetSpecificSimpleContainer err", err)
	}

	var simpleContainer *models.SimpleContainer

	simpleContainer, err = ah.getAzureContainerAsSimpleContainer(containerName)
	if err != nil {

		log.Debugf("container %s didn't exist, trying to create it: %s", containerName, err)

		_, _ = ah.getOrCreateContainer( containerName)
		simpleContainer, err = ah.getAzureContainerAsSimpleContainer(containerName)
		if err != nil {
			return nil, err
		}
	}

	subContainer, lastContainer := ah.generateSubContainers(simpleContainer, blobPrefix)

	if subContainer != nil {
		simpleContainer.ContainerSlice = append(simpleContainer.ContainerSlice, subContainer)
	}

	// return the "deepest" container.
	return lastContainer, nil
}


// Get container... or create a new one.
func (ah *AzureHandler) getOrCreateContainer(containerName string) (*storage.ContainerURL, error) {

	containerURL := ah.serviceURL.NewContainerURL( containerName)
	ctx := context.Background() // This example uses a never-expiring context
	_, err := containerURL.Create(ctx, storage.Metadata{}, storage.PublicAccessNone)

	if serr, ok := err.(storage.StorageError); ok { // This error is a Service-specific error
		if serr.ServiceCode() != storage.ServiceCodeContainerAlreadyExists {
		  return nil, err
		}
	}

	return &containerURL, nil
}

// GetContainerContentsOverChannel given a URL (ending in /) returns all the contents of the container over a channel
// This returns a COPY of the original source container but has been populated with *some* of the blobs/subcontainers in it.
func (ah *AzureHandler) GetContainerContentsOverChannel(sourceContainer models.SimpleContainer, blobChannel chan models.SimpleContainer) error {

	azureContainer, blobPrefix := containerutils.GetContainerAndBlobPrefix(&sourceContainer)

	containerURL := ah.serviceURL.NewContainerURL(azureContainer.Name)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()


	marker := storage.Marker{}

	// now we have the azure container and the prefix, we should be able to get a list of
	// SimpleContainers and SimpleBlobs to add this to original container.
	// Keep max results to 1000, can loop through and
	// params := storage.ListBlobsParameters{Prefix: blobPrefix, MaxResults: 1000}
	done := false
	for done == false {
		// copy of container, dont want to send back ever growing container via the channel.
		containerClone := sourceContainer

    	//azureContainer := ah.blobStorageClient.GetContainerReference(azureContainer.Name)
		blobListResponse, err := containerURL.ListBlobs( ctx, marker, storage.ListBlobsOptions{ Prefix: blobPrefix})
		if err != nil {
			log.Fatal("Error")
		}

		ah.populateSimpleContainer(blobListResponse, &containerClone, blobPrefix)

		// return entire container via channel.
		blobChannel <- containerClone

		// if marker, then keep going.
		if blobListResponse.NextMarker.NotDone() {
			marker = blobListResponse.NextMarker
		} else {
			done = true
		}
	}

	close(blobChannel)
	return nil
}

func (ah *AzureHandler) GeneratePresignedURL(blob *models.SimpleBlob) (string, error) {
	return "", nil
}

func (ah *AzureHandler) generateSubContainers(azureContainer *models.SimpleContainer, blobPrefix string) (*models.SimpleContainer, *models.SimpleContainer) {

	var containerToReturn *models.SimpleContainer
	var lastContainer *models.SimpleContainer
	doneFirst := false

	// strip off last /
	if len(blobPrefix) > 0 {
		blobPrefix = blobPrefix[:len(blobPrefix)-1]
		sp := strings.Split(blobPrefix, "/")

		for _, segment := range sp {
			container := models.NewSimpleContainer()
			container.Name = segment
			if !doneFirst {
				container.ParentContainer = azureContainer
				containerToReturn = container
				doneFirst = true
			} else {
				container.ParentContainer = lastContainer
				lastContainer.ContainerSlice = append(lastContainer.ContainerSlice, container)
			}

			lastContainer = container
		}
	} else {

		// just return existing container (lastContainer) and no subcontainer (containerToReturn)
		containerToReturn = nil
		lastContainer = azureContainer
	}

	return containerToReturn, lastContainer
}

func (ah *AzureHandler) getAzureContainerAsSimpleContainer(containerName string) (*models.SimpleContainer, error) {

	rootContainer := ah.GetRootContainer()

	for _, container := range rootContainer.ContainerSlice {
		if container.Name == containerName {
			return container, nil
		}
	}

	return nil, errors.New("Unable to find container")

}

// GetSpecificSimpleBlob given a URL (NOT ending in /) then get the SIMPLE blob that represents it.
// The DestName will be the last element of the URL, whether it's a real blobname or not.
// eg.  https://...../mycontainer/vdir1/vdir2/blobname    will return a DestName of "blobname" even though strictly
// speaking the true blobname is "vdir1/vdir2/blobname".
// Will revisit this if it causes a problem.
func (ah *AzureHandler) GetSpecificSimpleBlob(URL string) (*models.SimpleBlob, error) {
	// MUST be a better way to get the last character.
	if URL[len(URL)-2:len(URL)-1] == "/" {
		return nil, errors.New("Cannot end with a /")
	}
	_, containerName, blobName, pretendBlobName, err := ah.validateURL(URL)
	if err != nil {
		return nil, err
	}

	simpleContainer, err := ah.getAzureContainerAsSimpleContainer(containerName)

	b := models.SimpleBlob{}

	// if we're talking vdir1/vdir2/blobname then b.Name should be "blobname"
	b.Name = pretendBlobName
	b.Origin = models.Azure
	b.ParentContainer = simpleContainer
	b.BlobCloudName = blobName
	return &b, nil
}

// validateURL returns accountName, container Name, blob Name and error
// passes real URL such as https://myacct.blob.core.windows.net/mycontainer/vdir1/vdir2/blobPrefix
func (ah *AzureHandler) validateURL(URL string) (string, string, string, string, error) {

	lowerURL := strings.ToLower(URL)

	// ugly, do this properly!!! TODO(kpfaulkner)
	pruneCount := 0
	match, _ := regexp.MatchString("http://", lowerURL)
	if match {
		pruneCount = 7
	}

	match, _ = regexp.MatchString("https://", lowerURL)
	if match {
		pruneCount = 8
	}

	// trim protocol
	URL = URL[pruneCount:]
	sp := strings.Split(URL, "/")

	sp2 := strings.Split(sp[0], ".")
	accountName := sp2[0]

	var containerName string
	var blobName string

	if !ah.IsEmulator {
		containerName = sp[1]
		blobName = strings.Join(sp[2:], "/")
	} else {
		containerName = sp[2]
		blobName = strings.Join(sp[3:], "/")
	}

	pretendBlobName := sp[ len(sp) -1]
	return accountName, containerName, blobName,pretendBlobName, nil
}

// ReadBlob reads a blob of a given name from a particular SimpleContainer and returns the SimpleBlob
// The SimpleContainer is NOT necessarily a direct mapping to an Azure container but may be representing a virtual directory.
// ie we might have RootSimpleContainer -> SimpleContainer(myrealcontainer) -> SimpleContainer(vdir1) -> SimpleContainer(vdir2)
// and if the blobName is "myblob" then the REAL underlying Azure structure would be container == "myrealcontainer"
// and the blob name is vdir/vdir2/myblob
func (ah *AzureHandler) ReadBlob(container models.SimpleContainer, blobName string) models.SimpleBlob {
	var blob models.SimpleBlob

	return blob
}

// PopulateBlob. Used to read a blob IFF we already have a reference to it.
func (ah *AzureHandler) getBlobURL( containerName string, azureBlobName string) (*storage.BlobURL, error) {
	containerURL := ah.serviceURL.NewContainerURL(containerName)
	blobURL := containerURL.NewBlobURL(azureBlobName)

	return &blobURL, nil
}

// PopulateBlob. Used to read a blob IFF we already have a reference to it.
func (ah *AzureHandler) PopulateBlob(blob *models.SimpleBlob) error {
	azureContainerName := ah.generateAzureContainerName(*blob)
	azureBlobName := blob.BlobCloudName

	containerURL := ah.serviceURL.NewContainerURL(azureContainerName)
	blobURL := containerURL.NewBlobURL(azureBlobName)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()


	resp, err := blobURL.GetBlob(ctx, storage.BlobRange{}, storage.BlobAccessConditions{}, false)
	if err != nil {
		return  err
	}
	defer resp.Body().Close()

	// file stream for cache.
	var cacheFile *os.File

	// populate this to disk.
	if ah.cacheToDisk {

		cacheName := misc.GenerateCacheName(azureContainerName + blob.BlobCloudName)
		blob.DataCachedAtPath = ah.cacheLocation + "/" + cacheName
		log.Debugf("azure cache location is %s", blob.DataCachedAtPath)
		cacheFile, err = os.OpenFile(blob.DataCachedAtPath, os.O_WRONLY|os.O_CREATE, 0666)
		defer cacheFile.Close()

		if err != nil {
			log.Fatalf("Populate blob %s", err)
			return err
		}
	} else {
		blob.DataInMemory = []byte{}
	}

	// 100k buffer... way too small?
	buffer := make([]byte, 1024*100)
	numBytesRead := 0

	finishedProcessing := false
	for finishedProcessing == false {
		numBytesRead, err = resp.Body().Read(buffer)
		if err != nil {
			finishedProcessing = true
		}

		if numBytesRead <= 0 {
			finishedProcessing = true
			continue
		}

		// if we're caching, write to a file.
		if ah.cacheToDisk {
			_, err = cacheFile.Write(buffer[:numBytesRead])
			if err != nil {
				log.Fatal(err)
				return err
			}
		} else {

			// needs to go into a byte array. How do we expand a slice again?
			blob.DataInMemory = append(blob.DataInMemory, buffer[:numBytesRead]...)
		}
	}

	return nil
}

// generateAzureContainerName gets the REAL Azure container name for the simpleBlob
func (ah *AzureHandler) generateAzureContainerName(blob models.SimpleBlob) string {
	currentContainer := blob.ParentContainer

	for currentContainer.ParentContainer != nil {
		currentContainer = currentContainer.ParentContainer
	}
	return currentContainer.Name
}

func (ah *AzureHandler) WriteContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer) error {
	return nil
}

// WriteBlob writes a blob to an Azure container.
// The SimpleContainer is NOT necessarily a direct mapping to an Azure container but may be representing a virtual directory.
// ie we might have RootSimpleContainer -> SimpleContainer(myrealcontainer) -> SimpleContainer(vdir1) -> SimpleContainer(vdir2)
// and if the blobName is "myblob" then the REAL underlying Azure structure would be container == "myrealcontainer"
// and the blob name is vdir/vdir2/myblob
func (ah *AzureHandler) WriteBlob(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {

	log.Debugf("Azure WriteBlob destcont %s blob %s", destContainer.Name, sourceBlob.Name)

	var err error
	if ah.cacheToDisk {
		err = ah.writeBlobFromCache(destContainer, sourceBlob)
	} else {
		err = ah.writeBlobFromMemory(destContainer, sourceBlob)
	}

	if err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}

func (ah *AzureHandler) getContainerAndBlobNames(destContainer *models.SimpleContainer, sourceBlobName string) (string, string) {

	azureContainer, blobPrefix := containerutils.GetContainerAndBlobPrefix(destContainer)
	azureContainerName := azureContainer.Name

	var azureBlobName string

	if blobPrefix != "" {
		if misc.GetLastChar(blobPrefix) == "/" {
			azureBlobName = blobPrefix + sourceBlobName

		} else {
			azureBlobName = blobPrefix + "/" + sourceBlobName
		}
	} else {
		azureBlobName = sourceBlobName
	}

	return azureContainerName, azureBlobName
}

// writeBlobFromCache.. read the cache file and pass the byte slice onto the real writer.
func (ah *AzureHandler) writeBlobFromCache(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {
	azureContainerName, azureBlobName := ah.getContainerAndBlobNames(destContainer, sourceBlob.Name)

	_, err := ah.getOrCreateContainer( azureContainerName)
	if err != nil {
		log.Fatal(err)
		return err
	}

	log.Debugf("writeBlobFromCache container: %s blob: %s", azureContainerName, azureBlobName)
	// file stream for cache.
	var cacheFile *os.File

	// need to get cache dir from somewhere!
	cacheFile, err = os.OpenFile(sourceBlob.DataCachedAtPath, os.O_RDONLY, 0)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer cacheFile.Close()

	buffer := make([]byte, 1024*100)
	numBytesRead := 0
	blockIDList := []string{}
	finishedProcessing := false
	for finishedProcessing == false {
		numBytesRead, err = cacheFile.Read(buffer)
		if err != nil {
			finishedProcessing = true
			continue
		}

		if numBytesRead <= 0 {
			finishedProcessing = true
			continue
		}
		blockID, err := ah.writeMemoryToBlob(azureContainerName, azureBlobName, buffer[:numBytesRead])
		if err != nil {
			log.Fatal("Unable to write memory to blob ", err)
		}

		blockIDList = append(blockIDList, blockID)
	}

	// finialize the blob
	err = ah.putBlockIDList(azureContainerName, azureBlobName, blockIDList)
	if err != nil {
		log.Fatal("putBlockIDList failed ", err)
	}

	return nil
}

func (ah *AzureHandler) writeBlobFromMemory(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {

	azureContainerName, azureBlobName := ah.getContainerAndBlobNames(destContainer, sourceBlob.Name)

	_, err := ah.getOrCreateContainer(azureContainerName)
	if err != nil {
		log.Fatal(err)
		return err
	}

	totalBytes := len(sourceBlob.DataInMemory)
	bufferSize := 1024 * 100
	buffer := make([]byte, bufferSize)
	numBytesRead := 0
	bytesWritten := 0

	blockIDList := []string{}

	for bytesWritten < totalBytes {

		checkNumBytesToRead := bufferSize
		if totalBytes-numBytesRead < bufferSize {
			checkNumBytesToRead = totalBytes - numBytesRead
		}

		// write 100k at a time?
		// too small? too big?
		buffer = sourceBlob.DataInMemory[numBytesRead : numBytesRead+checkNumBytesToRead]

		blockID, err := ah.writeMemoryToBlob(azureContainerName, azureBlobName, buffer)
		if err != nil {
			log.Fatal("Unable to write memory to blob ", err)
		}

		blockIDList = append(blockIDList, blockID)
	}

	// finialize the blob
	err = ah.putBlockIDList(destContainer.Name, sourceBlob.Name, blockIDList)
	if err != nil {
		log.Fatal("putBlockIDList failed ", err)
	}

	return nil
}

func (ah *AzureHandler) putBlockIDList(containerName string, blobName string, blockIDList []string) error {

	log.Debugf("putBlockIDList container %s: blobName %s", containerName, blobName)

	containerURL := ah.serviceURL.NewContainerURL(containerName)
	blobURL := containerURL.NewBlockBlobURL(blobName)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	_, err := blobURL.PutBlockList(ctx, blockIDList, storage.BlobHTTPHeaders{}, storage.Metadata{}, storage.BlobAccessConditions{})
	return err

}

func (ah *AzureHandler) writeMemoryToBlob(containerName string, blobName string, buffer []byte) (string, error) {

	// generate hash of bytearray.
	blockID := ""

	//hasher := sha1.New()
	//hasher.Write(buffer)
	//blockID = hex.EncodeToString(hasher.Sum(nil))
	blockID = fmt.Sprintf("%s", uuid.NewV1())

	blockID = base64.StdEncoding.EncodeToString([]byte(blockID))

	containerURL := ah.serviceURL.NewContainerURL(containerName)
	blobURL := containerURL.NewBlockBlobURL(blobName)
	log.Debugf("blockID %s", blockID)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	_, err := blobURL.PutBlock(ctx, blockID, bytes.NewReader(buffer), storage.LeaseAccessConditions{})

	if err != nil {
		log.Fatal("Unable to PutBlock ", blockID)
	}

	return blockID, nil
}

// CreateContainer creates an Azure container.
// ie will only do ROOT level containers (ie REAL Azure container)
func (ah *AzureHandler) CreateContainer(containerName string) (models.SimpleContainer, error) {
	var container models.SimpleContainer

	_, err := ah.getOrCreateContainer(containerName)
	if err != nil {
		log.Fatal(err)
	}

	// dont get it...  creates an empty simplecontainer...  this needs to be relooked at!
	// only command uses it and discards it straight away, so might be ok.
	return container, nil
}

// GetContainer gets a container. Populating the subtree? OR NOT? hmmmm
func (ah *AzureHandler) GetContainer(containerName string) models.SimpleContainer {
	var container models.SimpleContainer

	return container
}

// GetContainerContents populates the passed container with the real contents.
// Can determine if the SimpleContainer is a real container or something virtual.
// We need to trace back to the root node and determine what is really a container and
// what is a blob.
//
// For Azure only the children of the root node can be a real azure container. Everything else
// is a blob or a blob pretending to have vdirs.
//
// TODO(kpfaulkner) use marker and get next lot of results when we have > 5000 blobs.
func (ah *AzureHandler) GetContainerContents(container *models.SimpleContainer) error {

	azureContainer, blobPrefix := containerutils.GetContainerAndBlobPrefix(container)

	// now we have the azure container and the prefix, we should be able to get a list of
	// SimpleContainers and SimpleBlobs to add this to original container.
	containerURL := ah.serviceURL.NewContainerURL(azureContainer.Name)
	ctx := context.Background() // This example uses a never-expiring context

	blobListResponse, err := containerURL.ListBlobs(ctx, storage.Marker{}, storage.ListBlobsOptions{Prefix: blobPrefix}  )
	if err != nil {
		log.Fatal("Error")
	}

	ah.populateSimpleContainer(blobListResponse, azureContainer, blobPrefix)

	return nil
}

// populateSimpleContainer takes a list of Azure blobs and breaks them into virtual directories (SimpleContainers) and
// SimpleBlob trees.
//
// vdir1/vdir2/blob1
// vdir1/blob2
// vdir1/vdir3/blob3
// blob4
func (ah *AzureHandler) populateSimpleContainer(blobListResponse *storage.ListBlobsResponse, container *models.SimpleContainer, blobPrefix string) {

	for _, blob := range blobListResponse.Blobs.Blob {

		log.Debugf("populateSimpleContainer blob %s", blob.Name)
		sp := strings.Split(blob.Name, "/")

		// if no / then no subdirs etc. Just add as is.
		if len(sp) == 1 {
			b := models.SimpleBlob{}
			b.Name = blob.Name
			b.Origin = container.Origin
			b.ParentContainer = container
			b.BlobCloudName = blob.Name
			// add to the blob slice within the container
			container.BlobSlice = append(container.BlobSlice, &b)
		} else {

			currentContainer := container
			// if slashes, then split into chunks and create accordingly.
			// skip last one since thats the blob name.
			spShort := sp[0 : len(sp)-1]
			for _, segment := range spShort {

				// check if container already has a subcontainer with appropriate name
				subContainer := ah.getSubContainer(currentContainer, segment)

				if subContainer != nil {
					// then we have a blob so add it to currentContainer
					currentContainer = subContainer
				}
			}

			b := models.SimpleBlob{}
			b.Name = sp[len(sp)-1]
			b.Origin = container.Origin
			b.ParentContainer = container
			b.BlobCloudName = blob.Name // cloud specific name... ie the REAL name.

			containerURL := ah.serviceURL.NewContainerURL(container.Name)
			blobURL := containerURL.NewBlobURL(blob.Name)
			b.URL = blobURL.String()
			currentContainer.BlobSlice = append(currentContainer.BlobSlice, &b)
			currentContainer.Populated = true
		}
	}
	container.Populated = true
}

// getSubContainer gets an existing subcontainer with parent of container and name of segment.
// otherwise it creates it, adds it to the parent container and returns the new one.
func (ah *AzureHandler) getSubContainer(container *models.SimpleContainer, segment string) *models.SimpleContainer {

	// MUST be a shorthand way of doing this. But still crawling in GO.
	for _, c := range container.ContainerSlice {
		if c.Name == segment {
			return c
		}
	}

	// create a new one.
	newContainer := models.SimpleContainer{}
	newContainer.Name = segment
	newContainer.Origin = container.Origin
	newContainer.ParentContainer = container
	container.ContainerSlice = append(container.ContainerSlice, &newContainer)
	return &newContainer
}
