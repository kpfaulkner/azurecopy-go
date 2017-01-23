package handlers

import (
	"azurecopy/azurecopy/models"
	"io/ioutil"

	log "github.com/Sirupsen/logrus"
)

type DropboxHandler struct {

	// determine if we're caching the blob to disk during copy operations.
	// or if we're keeping it in memory
	cacheToDisk   bool
	cacheLocation string

	// is this handler for the source or dest?
	IsSource bool
}

// NewDropboxHandler factory to create new one. Evil?
func NewDropboxHandler(isSource bool, cacheToDisk bool) (*DropboxHandler, error) {

	dh := new(DropboxHandler)
	dh.cacheToDisk = cacheToDisk
	dir, err := ioutil.TempDir("", "azurecopy")
	if err != nil {
		log.Fatalf("Unable to create temp directory %s", err)
	}

	dh.cacheLocation = dir
	dh.IsSource = isSource

	return dh, nil
}

// GetRootContainer gets root container of S3. Gets the list of buckets and THOSE are the immediate child containers here.
func (dh *DropboxHandler) GetRootContainer() models.SimpleContainer {
	container := models.SimpleContainer{}
	return container
}

// BlobExists checks if blob exists
func (dh *DropboxHandler) BlobExists(container models.SimpleContainer, blobName string) (bool, error) {
	return false, nil
}

// GetContainerContentsOverChannel given a URL (ending in /) returns all the contents of the container over a channel
// This is going to be inefficient from a memory allocation pov.
// Am still creating various structs that we strictly do not require for copying (all the tree structure etc) but this will
// at least help each cloud provider be consistent from a dev pov. Think it's worth the overhead. TODO(kpfaulkner) confirm :)
func (dh *DropboxHandler) GetContainerContentsOverChannel(sourceContainer models.SimpleContainer, blobChannel chan models.SimpleContainer) error {

	return nil
}

// GetSpecificSimpleContainer for S3 will be the bucket.
// Conversion from https://bucketname.s3.amazonaws.com/myblob to https://s3.amazonaws.com/bucketname/myblob is done first.
func (dh *DropboxHandler) GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error) {

	// return the "deepest" container.
	return nil, nil
}

// GetSpecificSimpleBlob given a URL (NOT ending in /) then get the SIMPLE blob that represents it.
func (dh *DropboxHandler) GetSpecificSimpleBlob(URL string) (*models.SimpleBlob, error) {

	return nil, nil
}

// ReadBlob reads a blob of a given name from a particular SimpleContainer and returns the SimpleBlob
// The SimpleContainer is NOT necessarily a direct mapping to an Azure container but may be representing a virtual directory.
// ie we might have RootSimpleContainer -> SimpleContainer(myrealcontainer) -> SimpleContainer(vdir1) -> SimpleContainer(vdir2)
// and if the blobName is "myblob" then the REAL underlying Azure structure would be container == "myrealcontainer"
// and the blob name is vdir/vdir2/myblob
func (dh *DropboxHandler) ReadBlob(container models.SimpleContainer, blobName string) models.SimpleBlob {
	var blob models.SimpleBlob

	return blob
}

// PopulateBlob. Used to read a blob IFF we already have a reference to it.
func (dh *DropboxHandler) PopulateBlob(blob *models.SimpleBlob) error {

	return nil
}

func (dh *DropboxHandler) WriteContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer) error {
	return nil
}

// WriteBlob writes a blob to an Azure container.
// The SimpleContainer is NOT necessarily a direct mapping to an Azure container but may be representing a virtual directory.
// ie we might have RootSimpleContainer -> SimpleContainer(myrealcontainer) -> SimpleContainer(vdir1) -> SimpleContainer(vdir2)
// and if the blobName is "myblob" then the REAL underlying Azure structure would be container == "myrealcontainer"
// and the blob name is vdir/vdir2/myblob
func (dh *DropboxHandler) WriteBlob(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {

	return nil
}

func (dh *DropboxHandler) CreateContainer(containerName string) (models.SimpleContainer, error) {
	var container models.SimpleContainer

	return container, nil
}

// GetContainer gets a container. Populating the subtree? OR NOT? hmmmm
func (dh *DropboxHandler) GetContainer(containerName string) models.SimpleContainer {
	var container models.SimpleContainer

	return container
}

// GetContainerContents populates the passed container with the real contents.
// Can determine if the SimpleContainer is a real container or something virtual.
// We need to trace back to the root node and determine what is really a container and
// what is a blob.
//
// For S3 only the children of the root node can be a real azure container. Everything else
// is a blob or a blob pretending to have vdirs.
func (dh *DropboxHandler) GetContainerContents(container *models.SimpleContainer) error {

	return nil
}

/* presign URL code.....  use it eventually.
 */

func (dh *DropboxHandler) GeneratePresignedURL(blob *models.SimpleBlob) (string, error) {

	return "", nil
}
