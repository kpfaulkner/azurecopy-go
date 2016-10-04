package handlers

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/azurehelper"
	"log"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/storage"
)

type AzureHandler struct {
	blobStorageClient storage.BlobStorageClient

	// determine if we're caching the blob to disk during copy operations.
	// or if we're keeping it in memory
	cacheToDisk   bool
	cacheLocation string

	// using the azure storage emulator
	useEmulator bool
}

// NewAzureHandler factory to create new one. Evil?
func NewAzureHandler(useEmulator bool, cacheToDisk bool) (*AzureHandler, error) {
	ah := new(AzureHandler)

	ah.useEmulator = useEmulator
	ah.cacheToDisk = cacheToDisk
	ah.cacheLocation = "c:/temp/cache/" // NFI... just making something up for now

	var err error
	var client storage.Client

	if useEmulator {
		client, err = storage.NewEmulatorClient()
	} else {
		client, err = storage.NewBasicClient("", "")
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
	azureContainerName := ah.generateAzureContainerName(blob)
	azureBlobName := blob.BlobCloudName

	log.Println("AzureHandler::PopulateBlob blobname " + azureBlobName)

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
		// need to get cache dir from somewhere!
		cacheFile, err = os.OpenFile(ah.cacheLocation+blob.Name, os.O_WRONLY|os.O_CREATE, 0)

		if err != nil {
			log.Fatal(err)
			return err
		}
	} else {
		blob.DataInMemory = []byte{}
	}

	// 10k buffer... way too small?
	buffer := make([]byte, 1024*10)
	numBytesRead := 0

	finishedProcessing := false
	for finishedProcessing == false {
		numBytesRead, err = sr.Read(buffer)
		if err != nil {
			finishedProcessing = true
			log.Println("READ ERROR ", err, numBytesRead)
		}

		if numBytesRead <= 0 {
			finishedProcessing = true
			continue
		}

		log.Println("read ", numBytesRead)

		// if we're caching, write to a file.
		if ah.cacheToDisk {
			_, err = cacheFile.Write(buffer[:numBytesRead])
			if err != nil {
				log.Fatal(err)
				return err
			}
		} else {

			log.Println("adding to memory ", numBytesRead)
			// needs to go into a byte array. How do we expand a slice again?
			blob.DataInMemory = append(blob.DataInMemory, buffer[:numBytesRead]...)
		}
	}

	log.Println("data in memory is ", len(blob.DataInMemory))

	return nil
}

// generateAzureContainerName gets the REAL Azure container name for the simpleBlob
func (ah *AzureHandler) generateAzureContainerName(blob *models.SimpleBlob) string {
	currentContainer := blob.ParentContainer

	for currentContainer.ParentContainer != nil {
		currentContainer = currentContainer.ParentContainer
	}
	return currentContainer.Name
}

// WriteBlob writes a blob to an Azure container.
// The SimpleContainer is NOT necessarily a direct mapping to an Azure container but may be representing a virtual directory.
// ie we might have RootSimpleContainer -> SimpleContainer(myrealcontainer) -> SimpleContainer(vdir1) -> SimpleContainer(vdir2)
// and if the blobName is "myblob" then the REAL underlying Azure structure would be container == "myrealcontainer"
// and the blob name is vdir/vdir2/myblob
func (ah *AzureHandler) WriteBlob(container models.SimpleContainer, blob models.SimpleBlob) {

}

func (ah *AzureHandler) CreateContainer(parentContainer models.SimpleContainer, containerName string) models.SimpleContainer {
	var container models.SimpleContainer

	return container
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
func (ah *AzureHandler) GetContainerContents(container *models.SimpleContainer, useEmulator bool) {

	azureContainer, blobPrefix := azurehelper.GetContainerAndBlobPrefix(container)

	// now we have the azure container and the prefix, we should be able to get a list of
	// SimpleContainers and SimpleBlobs to add this to original container.
	params := storage.ListBlobsParameters{}

	log.Println(blobPrefix)

	blobListResponse, err := ah.blobStorageClient.ListBlobs(azureContainer.Name, params)
	if err != nil {
		log.Println("oops")
		log.Fatal("Error")
	}

	ah.populateSimpleContainer(blobListResponse, container)
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
			log.Println("just added blob " + b.Name + " to container " + currentContainer.Name)

		}
	}
	container.Populated = true
}

// getSubContainer gets an existing subcontainer with parent of container and name of segment.
// otherwise it creates it, adds it to the parent container and returns the new one.
func (ah *AzureHandler) getSubContainer(container *models.SimpleContainer, segment string) *models.SimpleContainer {

	log.Println("AzureHandler::getSubContainer looking for ", segment)

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
