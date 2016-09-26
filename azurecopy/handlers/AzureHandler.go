package handlers

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/azurehelper"
	"log"

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
	params.Prefix = blobPrefix

	blobList, err := ah.blobStorageClient.ListBlobs(azureContainer.Name, params)
	if err != nil {
		log.Fatal("Error")
	}

}
