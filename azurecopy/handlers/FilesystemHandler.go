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

	dir, err := os.OpenFile(fh.rootContainerPath, os.O_RDONLY, 0)
	if err != nil {
		log.Fatal("ERR OpenFile ", err)
	}

	fileInfos, err := dir.Readdir(0)
	if err != nil {
		log.Fatal("ERR ReadDir", err)
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
			sc.Populated = false
			rootContainer.ContainerSlice = append(rootContainer.ContainerSlice, sc)

		} else {
			b := models.SimpleBlob{}
			b.Name = f.Name()
			b.ParentContainer = rootContainer
			b.Origin = models.Filesystem
			rootContainer.BlobSlice = append(rootContainer.BlobSlice, &b)

		}
	}
	rootContainer.Populated = true

	return *rootContainer
}

// ReadBlob in theory reads the blob. Given we're already dealing with a local filesystem DO we need to read it at all?
// No point keeping it in memory, local disk is good enough. Also any point making a copy to the cache directory?
// for now, just mark the blob as cached and point to original file dir.
func (fh *FilesystemHandler) ReadBlob(container models.SimpleContainer, blobName string) models.SimpleBlob {
	var blob models.SimpleBlob

	dirPath := fh.generateFullPath(&container)
	fullPath := dirPath + "/" + blobName

	blob.DataCachedAtPath = fullPath
	blob.BlobInMemory = false
	blob.Name = blobName
	blob.ParentContainer = &container
	blob.Origin = container.Origin
	blob.URL = fullPath
	return blob
}

// PopulateBlob. Used to read a blob IFF we already have a reference to it.
func (fh *FilesystemHandler) PopulateBlob(blob *models.SimpleBlob) error {

	blob.DataCachedAtPath = blob.URL
	blob.BlobInMemory = false

	return nil
}

// generateAzureContainerName gets the REAL Azure container name for the simpleBlob
func (fh *FilesystemHandler) generateAzureContainerName(blob *models.SimpleBlob) string {
	currentContainer := blob.ParentContainer
	return currentContainer.Name
}

// WriteBlob writes a blob to an Azure container.
// The SimpleContainer is NOT necessarily a direct mapping to an Azure container but may be representiing a virtual directory.
// ie we might have RootSimpleContainer -> SimpleContainer(myrealcontainer) -> SimpleContainer(vdir1) -> SimpleContainer(vdir2)
// and if the blobName is "myblob" then the REAL underlying Azure structure would be container == "myrealcontainer"
// and the blob name is vdir/vdir2/myblob
func (fh *FilesystemHandler) WriteBlob(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {
	log.Println("FilesystemHandler::WriteBlob ", sourceBlob.Name)

	fullPath := fh.generateFullPath(destContainer) + "/" + sourceBlob.Name

	if !sourceBlob.BlobInMemory {
		err := fh.copyFile(sourceBlob.DataCachedAtPath, fullPath)

		if err != nil {
			log.Fatal("FilesystemHandler::WriteBlob err ", err)
		}
	} else {
		// from memory.
		newFile, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE, 0)
		if err != nil {
			log.Fatal("FilesystemHandler::WriteBlob unable to open destination file", err)
		}

		var totalBytesWritten int = 0
		fileSize := len(sourceBlob.DataInMemory)

		for totalBytesWritten < fileSize {
			bytesWritten, err := newFile.Write(sourceBlob.DataInMemory[totalBytesWritten:])

			if err != nil {
				log.Fatal("FilesystemHandler::WriteBlob unable to open destination file", err)
			}
			totalBytesWritten += bytesWritten
		}
	}

	return nil
}

func (fh *FilesystemHandler) copyFile(sourceFile string, destFile string) error {
	cacheFile, err := os.OpenFile(sourceFile, os.O_RDONLY, 0)
	if err != nil {
		log.Fatal("FilesystemHandler::WriteBlob err ", err)
	}

	// location of destination blob.
	fullPath := destFile
	newFile, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE, 0)
	if err != nil {
		log.Fatal("FilesystemHandler::WriteBlob unable to open destination file", err)
	}

	buffer := make([]byte, 1024*100)
	numBytesRead := 0

	finishedProcessing := false
	for finishedProcessing == false {
		numBytesRead, err = cacheFile.Read(buffer)
		if err != nil {
			finishedProcessing = true
		}

		if numBytesRead <= 0 {
			finishedProcessing = true
			continue
		}

		_, err = newFile.Write(buffer[:numBytesRead])
		if err != nil {
			log.Fatal(err)
			return err
		}
	}

	return nil
}

func (fh *FilesystemHandler) WriteContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer) error {

	return nil
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

func (fh *FilesystemHandler) generateFullPath(container *models.SimpleContainer) string {

	path := container.Name
	currentContainer := container.ParentContainer
	for currentContainer != nil {
		path = currentContainer.Name + "/" + path
		currentContainer = currentContainer.ParentContainer
	}

	return fh.rootContainerPath + path
}

// GetContainerContents populates the container (directory) with the next level contents
// currently wont do recursive.
func (fh *FilesystemHandler) GetContainerContents(container *models.SimpleContainer, useEmulator bool) {

	fullPath := fh.generateFullPath(container)
	dir, err := os.OpenFile(fullPath, os.O_RDONLY, 0)
	if err != nil {
		log.Fatal("ERR OpenFile ", err)
	}

	fileInfos, err := dir.Readdir(0)
	if err != nil {
		log.Fatal("ERR ReadDir", err)
	}

	for _, f := range fileInfos {

		// determine if file or directory.
		// do we go recursive?
		if f.IsDir() {
			sc := models.NewSimpleContainer()
			sc.Name = f.Name()
			sc.Origin = models.Filesystem
			sc.ParentContainer = container
			sc.Populated = false
			container.ContainerSlice = append(container.ContainerSlice, sc)

		} else {
			b := models.SimpleBlob{}
			b.Name = f.Name()
			b.ParentContainer = container
			b.Origin = models.Filesystem
			b.URL = fh.generateFullPath(container) + "/" + b.Name
			container.BlobSlice = append(container.BlobSlice, &b)

		}
	}
	container.Populated = true

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

// GetSpecificSimpleContainer given a URL (ending in /) then get the SIMPLE container that represents it.
func (fh *FilesystemHandler) GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error) {

	return nil, nil
}

// GetSpecificSimpleBlob given a URL (NOT ending in /) then get the SIMPLE blob that represents it.
func (fh *FilesystemHandler) GetSpecificSimpleBlob(URL string) (*models.SimpleBlob, error) {
	return nil, nil

}
