package handlers

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/containerutils"
	"azurecopy/azurecopy/utils/helpers"
	"errors"
	"regexp"
	"strings"

	"fmt"
	"io/ioutil"

	log "github.com/Sirupsen/logrus"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
)

type DropboxHandler struct {

	// determine if we're caching the blob to disk during copy operations.
	// or if we're keeping it in memory
	cacheToDisk   bool
	cacheLocation string

	// is this handler for the source or dest?
	IsSource bool
}

var config *dropbox.Config

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

	config, err = helpers.SetupConnection()
	if err != nil {
		log.Fatalf("Unable to setup dropbox %s", err)
	}

	return dh, nil
}

// GetRootContainer gets root container of S3. Gets the list of buckets and THOSE are the immediate child containers here.
func (dh *DropboxHandler) GetRootContainer() models.SimpleContainer {
	container := models.SimpleContainer{}
	dbx := files.New(*config)

	arg := files.NewListFolderArg("")

	res, err := dbx.ListFolder(arg)
	if err != nil {
		log.Fatalf("Dropbox::GetRootContainer error %s", err)
	}

	log.Debugf("results are %s", res)
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

	dbx := files.New(*config)

	container, prefix := containerutils.GetContainerAndBlobPrefix(&sourceContainer)

	log.Debugf("container name %s", sourceContainer.Name)
	log.Debugf("prefix %s", prefix)

	var dirArg string
	if prefix != "" {
		dirArg = fmt.Sprintf("/%s/%s", container.Name, prefix)

	} else {
		dirArg = "/" + container.Name
	}

	log.Debugf("XXXDirArg is %s", dirArg)

	arg := files.NewListFolderArg(dirArg)
	arg.Recursive = true

	res, err := dbx.ListFolder(arg)
	if err != nil {
		log.Fatalf("Dropbox::GetRootContainer error %s", err)
	}

	log.Debugf("results are %s", res)

	/*
		done := false
		for done == false {
			// copy of container, dont want to send back ever growing container via the channel.
			containerClone := *azureContainer
			blobListResponse, err := ah.blobStorageClient.ListBlobs(containerClone.Name, params)
			if err != nil {
				log.Fatal("Error")
			}

			ah.populateSimpleContainer(blobListResponse, &containerClone)

			// return entire container via channel.
			blobChannel <- containerClone

			// if marker, then keep going.
			if blobListResponse.NextMarker != "" {
				params.Marker = blobListResponse.NextMarker
			} else {
				done = true
			}
		}

		close(blobChannel)
		return nil
	*/
	/*
		container := models.SimpleContainer{}
		processEntries(res, dirArg, &container)

		for res.HasMore {
			arg := files.NewListFolderContinueArg(res.Cursor)

			res, err = dbx.ListFolderContinue(arg)
			if err != nil {
				return nil, err
			}

			processEntries(res, dirArg, &container)
		}

		return &container, nil
	*/

	return nil
}

// GetSpecificSimpleContainer gets a specific dropbox directory/container
func (dh *DropboxHandler) GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error) {
	dbx := files.New(*config)

	dirArg := dh.getDirArg(URL)
	log.Debugf("DirArg is %s", dirArg)

	arg := files.NewListFolderArg(dirArg)

	// loop manually so we can track parent containers etc!!!!!!!!!!!!! DUH
	arg.Recursive = true

	res, err := dbx.ListFolder(arg)
	if err != nil {
		log.Fatalf("Dropbox::GetRootContainer error %s", err)
	}

	log.Debugf("results are %+v\n", res)

	for _, i := range res.Entries {
		log.Debugf("entry is %+v\n", i)
	}

	container := models.SimpleContainer{}
	container.Name = "" // getLastSegmentOfPath(dirArg)
	processEntries(res, dirArg, &container)

	for res.HasMore {
		arg := files.NewListFolderContinueArg(res.Cursor)

		res, err = dbx.ListFolderContinue(arg)
		if err != nil {
			return nil, err
		}

		processEntries(res, dirArg, &container)
	}

	// get the container we're actually after.
	wantedContainer, err := filterContainer(&container, dirArg)
	if err != nil {
		return nil, err
	}
	return wantedContainer, nil
}

// filterContainer gets the container we're after by checking dirArg  and pruning off the parent
// containers we're not after.
//
// ie rootContainer is literally the root, but maybe we were after /temp/dir1/dir2/  so we prune off
// the root, temp and dir1 parent containers and just return the dir2 container.
func filterContainer(rootContainer *models.SimpleContainer, dirArg string) (*models.SimpleContainer, error) {

	log.Debugf("filter container %s", dirArg)
	sp := strings.Split(dirArg, "/")

	container := rootContainer
	for _, dir := range sp {
		if dir != "" {
			log.Debugf("checking %s", dir)
			var childContainer *models.SimpleContainer

			foundChild := false
			// check children.
			for _, childContainer = range container.ContainerSlice {
				if childContainer.Name == dir {
					// found what we want.
					foundChild = true
					break
				}
			}

			if foundChild {
				container = childContainer
			} else {
				// haven't found what we want. Return error
				return nil, errors.New("Unable to find container")
			}
		}

	}

	return container, nil

}

/*
// populateSimpleContainer for S3 will be the bucket.
func (dh *DropboxHandler) populateSimpleContainer(path string, containerName string) (*models.SimpleContainer, error) {
	dbx := files.New(*config)

	arg := files.NewListFolderArg(path)

	// loop manually so we can track parent containers etc!!!!!!!!!!!!! DUH
	//arg.Recursive = true

	res, err := dbx.ListFolder(arg)
	if err != nil {
		log.Fatalf("Dropbox::populateSimpleContainer error %s", err)
	}

	log.Debugf("results are %s", res)

	container := models.SimpleContainer{}
	//container.Name = trimContainerName(containerName)
	//processEntries(res, dirArg, &container)

	for res.HasMore {
		arg := files.NewListFolderContinueArg(res.Cursor)

		res, err = dbx.ListFolderContinue(arg)
		if err != nil {
			return nil, err
		}

		processEntries(res, dirArg, &container)
	}

	return &container, nil
}
*/

func getLastSegmentOfPath(path string) string {

	log.Debugf("getLastSegmentOfPath %s", path)
	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	sp := strings.Split(path, "/")

	log.Debugf("split path %s", sp)
	return sp[len(sp)-1]

}

func trimContainerName(containerName string) string {

	path := containerName
	if strings.HasPrefix(path, "/") {
		path = strings.TrimPrefix(path, "/")
	}

	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}

	return path
}

func processEntries(results *files.ListFolderResult, dirArg string, rootContainer *models.SimpleContainer) {
	for _, i := range results.Entries {
		log.Debugf("res %s", i)
		switch f := i.(type) {
		case *files.FileMetadata:
			blob := models.SimpleBlob{}
			blob.Name = f.Name
			blob.URL = fmt.Sprintf("https://www.dropbox.com%s", f.PathDisplay) // NOT A REAL URL.... do we need it?
			blob.Origin = models.DropBox

			// adds to appropriate container. Will create intermediate containers if required.
			addToContainer(&blob, f.PathDisplay, rootContainer)

			//blob.ParentContainer = container
			//container.BlobSlice = append(container.BlobSlice, &blob)
			log.Debugf("FILE %s", f.Name)
			log.Debugf("path display %s", f.PathDisplay)

			/*
				case *files.FolderMetadata:
					c := models.SimpleContainer{}
					c.Name = f.Name
					c.ParentContainer = container
					c.URL = fmt.Sprintf("https://www.dropbox.com%s", f.PathDisplay) // NOT A REAL URL.... do we need it?
					c.Origin = models.DropBox
					container.ContainerSlice = append(container.ContainerSlice, &c)
					log.Debugf("DIR %s", f.Name) */

		}
	}

}

// addToContainer adds the blob to the rootContainer but will make appropriate child containers if required.
func addToContainer(blob *models.SimpleBlob, path string, rootContainer *models.SimpleContainer) {

	sp := strings.Split(path, "/")

	// just 1 length so member of root container.
	if len(sp) == 1 {
		rootContainer.BlobSlice = append(rootContainer.BlobSlice, blob)
		return
	}

	parentContainer := rootContainer

	for i := 0; i < len(sp)-1; i++ {
		segment := sp[i]
		log.Debugf("Container segment :%s:", segment)

		// dont want to add root container... already have it!
		if segment != "" {
			container := containerutils.GetContainerByName(parentContainer, segment)
			parentContainer = container
		}

	}

	// now add blob to parentContainer
	parentContainer.BlobSlice = append(parentContainer.BlobSlice, blob)
}

// getDirArg gets the directory argument for listing contents.
func (dh *DropboxHandler) getDirArg(URL string) string {
	lowerURL := strings.ToLower(URL)

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

	dirPrefix := "/" + strings.Join(sp[1:], "/")

	if dirPrefix == "/" {
		return ""
	}

	return dirPrefix
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
	log.Debugf("populateblob %s", blob.Name)
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

// need to populate....   for Dropbox GetSpecificSimpleContainer is doing too much already!
func (dh *DropboxHandler) GetContainerContents(container *models.SimpleContainer) error {

	return nil
}

/* presign URL code.....  use it eventually.
 */

func (dh *DropboxHandler) GeneratePresignedURL(blob *models.SimpleBlob) (string, error) {

	return "", nil
}
