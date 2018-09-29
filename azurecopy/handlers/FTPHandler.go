package handlers

import (
	"azurecopy/azurecopy/models"
	"path/filepath"
	"strings"

	"os"
	"path"

	log "github.com/Sirupsen/logrus"
	"github.com/jlaffaye/ftp"
)

// FTPHandler basic data structure for FTP handling.
type FTPHandler struct {

	// root directory we're dealing with.
	// This is the prefix for any containers. For example, if we're after c:\temp\data\s3\  then the rootContainerPath is really c:\temp\data\
	// and the container used later will be s3. Need to revisit this structure later if gets too confusing.
	rootContainerPath string

	// basePath used for prefix.
	basePath string

	// container in URL
	container string

	// is this source or dest handler?
	IsSource bool

	// client connection
	client *ftp.ServerConn
}

// NewFTPHandler factory to create new one. Evil?
func NewFTPHandler(address string, username string, password string, isSource bool) (*FTPHandler, error) {

	fh := new(FTPHandler)
	fh.IsSource = isSource

	client, err := ftp.Dial(address)
	if err != nil {
		return nil, err
	}

	if err := client.Login(username, password); err != nil {
		return nil, err
	}

	fh.client = client
	return fh, nil
}

// gets root container. This will get containers/blobs in this container
// NOT recursive.
func (fh *FTPHandler)  GetRootContainer() models.SimpleContainer {
	//entries, _ := fh.client.List("")

	return models.SimpleContainer{}
}

// create container.
func (fh *FTPHandler) CreateContainer(containerName string) (models.SimpleContainer, error) {
return models.SimpleContainer{}, nil
}

// GetSpecificSimpleContainer given a URL (ending in /) then get the SIMPLE container that represents it.
// does not have to have all blobs populated in it. Those can be retrieved later via GetContainerContentsOverChannel
// This is up to specific handlers. Currently (for example). For FTP if the url is myftpsite.com/dir1/dir2/dir3/ then it
// will return a SimpleContainer representing dir3 with all its contents.6
func (fh *FTPHandler) GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error) {

}

// GetContainerContentsOverChannel given a URL (ending in /) returns all the contents of the container over a channel
// GetContainerContentsOverChannel given a URL (ending in /) returns all the contents of the container over a channel
// This returns a COPY of the original source container but has been populated with *some* of the blobs/subcontainers in it.
func (fh *FTPHandler) GetContainerContentsOverChannel(sourceContainer models.SimpleContainer, blobChannel chan models.SimpleContainer) error {

}

// GetSpecificSimpleBlob given a URL (NOT ending in /) then get the SIMPLE blob that represents it.
// The DestName will be the last element of the URL, whether it's a real blobname or not.
// eg.  https://...../mycontainer/vdir1/vdir2/blobname    will return a DestName of "blobname" even though strictly
// speaking the true blobname is "vdir1/vdir2/blobname".
// Will revisit this if it causes a problem.
func (fh *FTPHandler) GetSpecificSimpleBlob(URL string) (*models.SimpleBlob, error) {

}

// Given a container and a blob name, read the blob.
func (fh *FTPHandler) ReadBlob(container models.SimpleContainer, blobName string) models.SimpleBlob {

}

// Does blob exist
func (fh *FTPHandler) BlobExists(container models.SimpleContainer, blobName string) (bool, error) {

}

// if we already have a reference to a SimpleBlob, then read it and populate it.
func (fh *FTPHandler) PopulateBlob(blob *models.SimpleBlob) error {

}

// given a container and blob, write blob.
func (fh *FTPHandler) WriteBlob(container *models.SimpleContainer, blob *models.SimpleBlob) error {

}

// write a container (and subcontents) to the appropriate data store
func (fh *FTPHandler) WriteContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer) error {

}

// Gets a container. Populating the subtree? OR NOT? hmmmm
func (fh *FTPHandler)  GetContainer(containerName string) models.SimpleContainer {

}

// populates container with data.
func (fh *FTPHandler) GetContainerContents(container *models.SimpleContainer) error {

}

// generates presigned URL so Azure can access blob for CopyBlob flag operation.
func (fh *FTPHandler)  GeneratePresignedURL(blob *models.SimpleBlob) (string, error) {

}
