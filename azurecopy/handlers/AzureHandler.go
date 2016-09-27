package handlers

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/azurehelper"
	"fmt"
	"log"
	"strings"

	"github.com/Azure/azure-sdk-for-go/storage"
)

type AzureHandler struct {
	blobStorageClient storage.BlobStorageClient
}

// NewAzureHandler factory to create new one. Evil?
func NewAzureHandler(useEmulator bool) *AzureHandler {
	ah := new(AzureHandler)

	var err error
	var client storage.Client

	if useEmulator {
		client, err = storage.NewEmulatorClient()
	} else {
		client, err = storage.NewBasicClient("", "")
	}

	if err != nil {
		// indicate error somehow..  still trying to figure that out with GO.
	}

	ah.blobStorageClient = client.GetBlobService()
	return ah
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

		rootContainer.ContainerSlice = append(rootContainer.ContainerSlice, *sc)
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
	// params.Prefix = blobPrefix

	fmt.Println(blobPrefix)

	blobListResponse, err := ah.blobStorageClient.ListBlobs(azureContainer.Name, params)
	if err != nil {
		fmt.Println("oops")
		log.Fatal("Error")
	}

	ah.populateSimpleContainer(blobListResponse, container)

	fmt.Println("blah blah")
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

			// add to the blob slice within the container
			container.BlobSlice = append(container.BlobSlice, b)
		} else {

			currentContainer := container
			// if slashes, then split into chunks and create accordingly.
			// skip last one since thats the blob name.
			spShort := sp[0 : len(sp)-1]

			for _, segment := range spShort {

				// check if container already has a subcontainer with appropriate name
				subContainer := ah.getSubContainer(currentContainer, segment)

				if subContainer == nil {
					// then we have a blob.
				}
				currentContainer = subContainer
			}

		}
	}
}

// getSubContainer gets an existing subcontainer with parent of container and name of segment.
// otherwise it creates it, adds it to the parent container and returns the new one.
func (ah *AzureHandler) getSubContainer(container *models.SimpleContainer, segment string) *models.SimpleContainer {

	// MUST be a shorthand way of doing this. But still crawling in GO.
	for _, c := range container.ContainerSlice {
		if c.Name == segment {
			return &c
		}
	}

	// create a new one.
	newContainer := models.SimpleContainer{}
	newContainer.Name = segment
	newContainer.Origin = container.Origin
	newContainer.ParentContainer = container
	container.ContainerSlice = append(container.ContainerSlice, newContainer)

	return &newContainer
}
