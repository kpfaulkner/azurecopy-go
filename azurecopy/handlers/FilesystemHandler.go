package handlers

import (
	"azurecopy/azurecopy/models"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/storage"
)

// FilesystemHandler basic data structure for FS handling.
type FilesystemHandler struct {

	// root directory we're dealing with.
	rootContainerPath string
}

// NewFilesystemHandler factory to create new one. Evil?
func NewFilesystemHandler(rootContainerPath string) (*FilesystemHandler, error) {
	fh := new(FilesystemHandler)
	fh.rootContainerPath = rootContainerPath

	return fh, nil
}

// GetRootContainer gets root container of Azure. In reality there isn't a root container, but this would basically be a SimpleContainer
// that has the containerSlice populated with the real Azure containers.
func (fh *FilesystemHandler) GetRootContainer() models.SimpleContainer {

	dir, err := os.OpenFile(fh.rootContainerPath, os.O_WRONLY, 0)
	if err != nil {
		log.Fatal(err)
	}

	fileInfos, err := dir.Readdir(0)
	if err != nil {
		log.Fatal(err)
	}

	rootContainer := models.NewSimpleContainer()
	for _, f := range fileInfos {

		// determine if file or directory.
		// do we go recursive?
		if f.IsDir() {
			sc := models.NewSimpleContainer()
			sc.Name = f.Name()
			sc.Origin = models.Filesystem
			sc.ParentContainer = rootContainer
			rootContainer.ContainerSlice = append(rootContainer.ContainerSlice, sc)

		} else {
			b := models.SimpleBlob{}
			b.Name = f.Name()
			b.ParentContainer = rootContainer
			b.Origin = models.Filesystem
			rootContainer.BlobSlice = append(rootContainer.BlobSlice, &b)

		}

	}

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

// generateAzureContainerName gets the REAL Azure container name for the simpleBlob
func (fh *FilesystemHandler) generateAzureContainerName(blob *models.SimpleBlob) string {
	currentContainer := blob.ParentContainer
	return currentContainer.Name
}

// WriteBlob writes a blob to an Azure container.
// The SimpleContainer is NOT necessarily a direct mapping to an Azure container but may be representing a virtual directory.
// ie we might have RootSimpleContainer -> SimpleContainer(myrealcontainer) -> SimpleContainer(vdir1) -> SimpleContainer(vdir2)
// and if the blobName is "myblob" then the REAL underlying Azure structure would be container == "myrealcontainer"
// and the blob name is vdir/vdir2/myblob
func (fh *FilesystemHandler) WriteBlob(container models.SimpleContainer, blob models.SimpleBlob) {

}

func (fh *FilesystemHandler) CreateContainer(parentContainer models.SimpleContainer, containerName string) models.SimpleContainer {
	var container models.SimpleContainer

	return container
}

// GetContainer gets a container. Populating the subtree? OR NOT? hmmmm
func (fh *FilesystemHandler) GetContainer(containerName string) models.SimpleContainer {
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
func (fh *FilesystemHandler) GetContainerContents(container *models.SimpleContainer, useEmulator bool) {

}

// populateSimpleContainer takes a list of Azure blobs and breaks them into virtual directories (SimpleContainers) and
// SimpleBlob trees.
//
// vdir1/vdir2/blob1
// vdir1/blob2
// vdir1/vdir3/blob3
// blob4
func (fh *FilesystemHandler) populateSimpleContainer(blobListResponse storage.BlobListResponse, container *models.SimpleContainer) {

}

// getSubContainer gets an existing subcontainer with parent of container and name of segment.
// otherwise it creates it, adds it to the parent container and returns the new one.
func (fh *FilesystemHandler) getSubContainer(container *models.SimpleContainer, segment string) *models.SimpleContainer {

	// create a new one.
	newContainer := models.SimpleContainer{}
	return &newContainer
}
