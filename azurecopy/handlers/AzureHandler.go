package handlers

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/azurehelper"
	"encoding/base64"
	"errors"
	"os"
	"regexp"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/Azure/azure-sdk-for-go/storage"
)

type AzureHandler struct {
	blobStorageClient storage.BlobStorageClient

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
	ah.cacheLocation = "c:/temp/cache/" // NFI... just making something up for now
	ah.IsSource = isSource
	ah.IsEmulator = isEmulator
	var err error
	var client storage.Client

	if accountName == "" && accountKey == "" {
		client, err = storage.NewEmulatorClient()
	} else {
		client, err = storage.NewBasicClient(accountName, accountKey)
	}

	if err != nil {

		// indicate error somehow..  still trying to figure that out with GO.
		return nil, err
	}

	ah.blobStorageClient = client.GetBlobService()
	return ah, nil
}

// GetRootContainer gets root container of Azure. In reality there isn't a root container, but this would basically be a SimpleContainer
// that has the containerSlice populated with the real Azure containers.
func (ah *AzureHandler) GetRootContainer() models.SimpleContainer {

	log.Debugf("GetRootContainer")

	params := storage.ListContainersParameters{}
	containerResponse, err := ah.blobStorageClient.ListContainers(params)

	if err != nil {
		// NFI.
	}

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

	azureContainerName, azureBlobName := ah.getContainerAndBlobNames(&container, blobName)
	exists, err := ah.blobStorageClient.BlobExists(azureContainerName, azureBlobName)
	if err != nil {
		return false, err
	}

	return exists, nil

}

// GetSpecificSimpleContainer given a URL (ending in /) then get the SIMPLE container that represents it.
// returns the container of the last most part of the url.
// eg. if the url was https://myacct.blob.core.windows.net/realazurecontainer/vdir1/vdir2/  then the simple container
// returned is vdir.
func (ah *AzureHandler) GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error) {

	log.Debugf("GetSpecificSimpleContainer %s", URL)
	lastChar := URL[len(URL)-1:]
	// MUST be a better way to get the last character.
	if lastChar != "/" {
		return nil, errors.New("Needs to end with a /")
	}

	_, containerName, blobPrefix, err := ah.validateURL(URL)
	if err != nil {
		log.Fatal("GetSpecificSimpleContainer err", err)
	}

	container, err := ah.getAzureContainer(containerName)
	if err != nil {

		// cant get container, create it.
		err = ah.blobStorageClient.CreateContainer(containerName, storage.ContainerAccessTypeBlob)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Debugf("Container: %s blobPrefix: %s", container, blobPrefix)
	subContainer, lastContainer := ah.generateSubContainers(container, blobPrefix)

	if subContainer != nil {
		container.ContainerSlice = append(container.ContainerSlice, subContainer)
	}

	log.Debugf("GetSpecificSimpleContainer returning %s", lastContainer.Name)

	// return the "deepest" container.
	return lastContainer, nil
}

func (ah *AzureHandler) generateSubContainers(azureContainer *models.SimpleContainer, blobPrefix string) (*models.SimpleContainer, *models.SimpleContainer) {

	log.Debugf("generateSubContainers %s : prefix %s", azureContainer.Name, blobPrefix)

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

func (ah *AzureHandler) getAzureContainer(containerName string) (*models.SimpleContainer, error) {

	log.Debugf("getAzureContainer %s", containerName)
	rootContainer := ah.GetRootContainer()

	for _, container := range rootContainer.ContainerSlice {
		if container.Name == containerName {
			return container, nil
		}
	}

	return nil, errors.New("Unable to find container")

}

// GetSpecificSimpleBlob given a URL (NOT ending in /) then get the SIMPLE blob that represents it.
func (ah *AzureHandler) GetSpecificSimpleBlob(URL string) (*models.SimpleBlob, error) {
	// MUST be a better way to get the last character.
	if URL[len(URL)-2:len(URL)-1] == "/" {
		return nil, errors.New("Cannot end with a /")
	}

	return nil, nil
}

// validateURL returns accountName, container Name, blob Name and error
// passes real URL such as https://myacct.blob.core.windows.net/mycontainer/blobPrefix
func (ah *AzureHandler) validateURL(URL string) (string, string, string, error) {

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
	lowerURL = lowerURL[pruneCount:]
	sp := strings.Split(lowerURL, "/")

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

	return accountName, containerName, blobName, nil
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
func (ah *AzureHandler) PopulateBlob(blob *models.SimpleBlob) error {
	azureContainerName := ah.generateAzureContainerName(*blob)
	azureBlobName := blob.BlobCloudName

	log.Debugf("AzureHandler::PopulateBlob blobname %s", azureBlobName)

	sr, err := ah.blobStorageClient.GetBlob(azureContainerName, azureBlobName)

	if err != nil {
		log.Fatal(err)
		return err
	}
	defer sr.Close()

	// file stream for cache.
	var cacheFile *os.File

	// populate this to disk.
	if ah.cacheToDisk {

		blob.DataCachedAtPath = ah.cacheLocation + blob.Name
		cacheFile, err = os.OpenFile(blob.DataCachedAtPath, os.O_WRONLY|os.O_CREATE, 0)

		if err != nil {
			log.Fatal(err)
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
		numBytesRead, err = sr.Read(buffer)
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

			log.Debugf("adding to memory %d", numBytesRead)
			// needs to go into a byte array. How do we expand a slice again?
			blob.DataInMemory = append(blob.DataInMemory, buffer[:numBytesRead]...)
		}
	}

	log.Debugf("data in memory is %d", len(blob.DataInMemory))

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

	log.Debugf("AzureHandler::WriteBlob")

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

	azureContainer, blobPrefix := azurehelper.GetContainerAndBlobPrefix(destContainer)
	azureContainerName := azureContainer.Name

	var azureBlobName string

	if blobPrefix != "" {
		azureBlobName = blobPrefix + "/" + sourceBlobName
	} else {
		azureBlobName = sourceBlobName
	}

	return azureContainerName, azureBlobName
}

// writeBlobFromCache.. read the cache file and pass the byte slice onto the real writer.
func (ah *AzureHandler) writeBlobFromCache(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {
	log.Debugf("writeBlobFromCache %s", sourceBlob.DataCachedAtPath)

	azureContainerName, azureBlobName := ah.getContainerAndBlobNames(destContainer, sourceBlob.Name)
	err := ah.blobStorageClient.CreateContainer(azureContainerName, storage.ContainerAccessTypePrivate)
	if err != nil {
		log.Fatal(err)
		return err
	}

	// file stream for cache.
	var cacheFile *os.File

	// need to get cache dir from somewhere!
	cacheFile, err = os.OpenFile(sourceBlob.DataCachedAtPath, os.O_RDONLY, 0)
	if err != nil {
		log.Fatal(err)
		return err
	}

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
		log.Debugf("buffer length %d", len(buffer))
		blockID, err := ah.writeMemoryToBlob(azureContainerName, azureBlobName, buffer[:numBytesRead])
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

func (ah *AzureHandler) writeBlobFromMemory(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {

	azureContainerName, azureBlobName := ah.getContainerAndBlobNames(destContainer, sourceBlob.Name)
	err := ah.blobStorageClient.CreateContainer(azureContainerName, storage.ContainerAccessTypePrivate)
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

	log.Debug("AzureHandler::putBlockIDList")
	blockSlice := ah.generateBlockSlice(blockIDList)
	if err := ah.blobStorageClient.PutBlockList(containerName, blobName, blockSlice); err != nil {
		log.Fatal("putBlockIDList failed ", err)
	}

	return nil
}

func (ah *AzureHandler) generateBlockSlice(blockIDList []string) []storage.Block {
	blockSlice := []storage.Block{}
	for _, block := range blockIDList {
		b := storage.Block{}
		b.ID = block
		b.Status = storage.BlockStatusLatest
		blockSlice = append(blockSlice, b)
	}
	return blockSlice
}

func (ah *AzureHandler) writeMemoryToBlob(containerName string, blobName string, buffer []byte) (string, error) {

	// generate hash of bytearray.
	blockID := ""

	//hasher := sha1.New()
	//hasher.Write(buffer)
	//blockID = hex.EncodeToString(hasher.Sum(nil))

	blockID = base64.StdEncoding.EncodeToString(buffer)

	log.Debugf("Creating blockID %s", blockID)
	err := ah.blobStorageClient.PutBlock(containerName, blobName, blockID, buffer)
	if err != nil {
		log.Fatal("Unable to PutBlock ", blockID)
	}

	return blockID, nil
}

// CreateContainer creates an Azure container.
// ie will only do ROOT level containers (ie REAL Azure container)
func (ah *AzureHandler) CreateContainer(containerName string) (models.SimpleContainer, error) {
	var container models.SimpleContainer
	log.Debugf("CreateContainer %s", containerName)

	err := ah.blobStorageClient.CreateContainer(containerName, storage.ContainerAccessTypeBlob)
	if err != nil {
		log.Fatal(err)
	}

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
func (ah *AzureHandler) GetContainerContents(container *models.SimpleContainer) {

	azureContainer, blobPrefix := azurehelper.GetContainerAndBlobPrefix(container)

	// now we have the azure container and the prefix, we should be able to get a list of
	// SimpleContainers and SimpleBlobs to add this to original container.
	params := storage.ListBlobsParameters{Prefix: blobPrefix}

	log.Debugf("Blob Prefix %s", blobPrefix)

	blobListResponse, err := ah.blobStorageClient.ListBlobs(azureContainer.Name, params)
	if err != nil {
		log.Fatal("Error")
	}

	ah.populateSimpleContainer(blobListResponse, azureContainer)
}

// populateSimpleContainer takes a list of Azure blobs and breaks them into virtual directories (SimpleContainers) and
// SimpleBlob trees.
//
// vdir1/vdir2/blob1
// vdir1/blob2
// vdir1/vdir3/blob3
// blob4
func (ah *AzureHandler) populateSimpleContainer(blobListResponse storage.BlobListResponse, container *models.SimpleContainer) {

	for _, blob := range blobListResponse.Blobs {

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
			b.URL = ah.blobStorageClient.GetBlobURL(container.Name, blob.Name)
			currentContainer.BlobSlice = append(currentContainer.BlobSlice, &b)
			currentContainer.Populated = true
			log.Debugf("just added blob %s to container", b.Name, currentContainer.Name)

		}
	}
	container.Populated = true
}

// getSubContainer gets an existing subcontainer with parent of container and name of segment.
// otherwise it creates it, adds it to the parent container and returns the new one.
func (ah *AzureHandler) getSubContainer(container *models.SimpleContainer, segment string) *models.SimpleContainer {

	log.Debugf("AzureHandler::getSubContainer looking for %s", segment)

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
