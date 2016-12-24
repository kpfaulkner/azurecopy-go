package handlers

import (
	"azurecopy/azurecopy/models"
	"errors"

	log "github.com/Sirupsen/logrus"
)

type S3Handler struct {

	// determine if we're caching the blob to disk during copy operations.
	// or if we're keeping it in memory
	cacheToDisk   bool
	cacheLocation string

	// is this handler for the source or dest?
	IsSource bool
}

// NewS3Handler factory to create new one. Evil?
func NewS3Handler(accessID string, accessSecret string, isSource bool, cacheToDisk bool) (*S3Handler, error) {
	sh := new(S3Handler)

	sh.cacheToDisk = cacheToDisk
	sh.cacheLocation = "c:/temp/cache/" // NFI... just making something up for now
	sh.IsSource = isSource

	return sh, nil
}

// GetRootContainer gets root container of Azure. In reality there isn't a root container, but this would basically be a SimpleContainer
// that has the containerSlice populated with the real Azure containers.
func (sh *S3Handler) GetRootContainer() models.SimpleContainer {

	rootContainer := models.NewSimpleContainer()

	return *rootContainer
}

// GetSpecificSimpleContainer for S3 will be the bucket.
// Assuming format of URL is https://bucketname.s3.amazonaws.com/myblob
func (sh *S3Handler) GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error) {

	log.Debugf("GetSpecificSimpleContainer %s", URL)

	lastContainer := models.NewSimpleContainer()

	// return the "deepest" container.
	return lastContainer, nil
}

// GetSpecificSimpleBlob given a URL (NOT ending in /) then get the SIMPLE blob that represents it.
func (sh *S3Handler) GetSpecificSimpleBlob(URL string) (*models.SimpleBlob, error) {
	// MUST be a better way to get the last character.
	if URL[len(URL)-2:len(URL)-1] == "/" {
		return nil, errors.New("Cannot end with a /")
	}

	return nil, nil
}

// ReadBlob reads a blob of a given name from a particular SimpleContainer and returns the SimpleBlob
// The SimpleContainer is NOT necessarily a direct mapping to an Azure container but may be representing a virtual directory.
// ie we might have RootSimpleContainer -> SimpleContainer(myrealcontainer) -> SimpleContainer(vdir1) -> SimpleContainer(vdir2)
// and if the blobName is "myblob" then the REAL underlying Azure structure would be container == "myrealcontainer"
// and the blob name is vdir/vdir2/myblob
func (sh *S3Handler) ReadBlob(container models.SimpleContainer, blobName string) models.SimpleBlob {
	var blob models.SimpleBlob

	return blob
}

// PopulateBlob. Used to read a blob IFF we already have a reference to it.
func (sh *S3Handler) PopulateBlob(blob *models.SimpleBlob) error {

	return nil
}

func (sh *S3Handler) WriteContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer) error {
	return nil
}

// WriteBlob writes a blob to an Azure container.
// The SimpleContainer is NOT necessarily a direct mapping to an Azure container but may be representing a virtual directory.
// ie we might have RootSimpleContainer -> SimpleContainer(myrealcontainer) -> SimpleContainer(vdir1) -> SimpleContainer(vdir2)
// and if the blobName is "myblob" then the REAL underlying Azure structure would be container == "myrealcontainer"
// and the blob name is vdir/vdir2/myblob
func (sh *S3Handler) WriteBlob(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {

	return nil
}

func (sh *S3Handler) CreateContainer(containerName string) (models.SimpleContainer, error) {
	var container models.SimpleContainer

	return container, nil
}

// GetContainer gets a container. Populating the subtree? OR NOT? hmmmm
func (ah *S3Handler) GetContainer(containerName string) models.SimpleContainer {
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
func (sh *S3Handler) GetContainerContents(container *models.SimpleContainer) {

}
