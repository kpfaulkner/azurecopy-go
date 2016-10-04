package handlers

import (
	"azurecopy/azurecopy/models"
)

type FilesystemHandler struct {
	dummy string
}

// NewFilesystemHandler factory to create new one. Evil?
func NewFilesystemHandler(useEmulator bool, cacheToDisk bool) *FilesystemHandler {
	fh := new(FilesystemHandler)
	return fh
}

// GetRootContainer gets root container of Azure. In reality there isn't a root container, but this would basically be a SimpleContainer
// that has the containerSlice populated with the real Azure containers.
func (fh *FilesystemHandler) GetRootContainer() models.SimpleContainer {
	rootContainer := models.NewSimpleContainer()
	return *rootContainer
}

// ReadBlob reads a blob of a given name from a particular SimpleContainer and returns the SimpleBlob
// The SimpleContainer is NOT necessarily a direct mapping to an Azure container but may be representing a virtual directory.
// ie we might have RootSimpleContainer -> SimpleContainer(myrealcontainer) -> SimpleContainer(vdir1) -> SimpleContainer(vdir2)
// and if the blobName is "myblob" then the REAL underlying Azure structure would be container == "myrealcontainer"
// and the blob name is vdir/vdir2/myblob
func (fh *FilesystemHandler) ReadBlob(container models.SimpleContainer, blobName string) models.SimpleBlob {
	var blob models.SimpleBlob

	return blob
}

// PopulateBlob. Used to read a blob IFF we already have a reference to it.
func (fh *FilesystemHandler) PopulateBlob(blob *models.SimpleBlob) error {

	return nil
}

// WriteBlob writes a blob to an Azure container.
// The SimpleContainer is NOT necessarily a direct mapping to an Azure container but may be representing a virtual directory.
// ie we might have RootSimpleContainer -> SimpleContainer(myrealcontainer) -> SimpleContainer(vdir1) -> SimpleContainer(vdir2)
// and if the blobName is "myblob" then the REAL underlying Azure structure would be container == "myrealcontainer"
// and the blob name is vdir/vdir2/myblob
func (fh *FilesystemHandler) WriteBlob(container models.SimpleContainer, blob models.SimpleBlob) {

}

// CreateContainer
func (fh *FilesystemHandler) CreateContainer(parentContainer models.SimpleContainer, containerName string) models.SimpleContainer {
	var container models.SimpleContainer

	return container
}

// GetContainer ets a container. Populating the subtree? OR NOT? hmmmm
func (fh *FilesystemHandler) GetContainer(containerName string) models.SimpleContainer {
	var container models.SimpleContainer

	return container
}

// GetContainerContents populates the passed container with the real contents.
func (fh *FilesystemHandler) GetContainerContents(container *models.SimpleContainer, useEmulator bool) {

}
