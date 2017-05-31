package handlers

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/blobutils"
	"azurecopy/azurecopy/utils/containerutils"
	"azurecopy/azurecopy/utils/helpers"
	"errors"
	"io/ioutil"
	"regexp"
	"strings"

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
// This returns a COPY of the original source container but has been populated with *some* of the blobs/subcontainers in it.
// The way the dropbox code is written the sourceContainer should actually be holding ALL the blobs already so there is nothing to do except
// push to channel.
//
// This might be an issue later with massive number of dropbox blobs. FIXME(kpfaulkner)
func (dh *DropboxHandler) GetContainerContentsOverChannel(sourceContainer models.SimpleContainer, blobChannel chan models.SimpleContainer) error {

	log.Debugf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
	log.Debugf("dropbox::GetContainerContentsOverChannel container %s", sourceContainer.Name)
	defer close(blobChannel)

	sourceContainer.DisplayContainer("")

	blobChannel <- sourceContainer

	/*

		dbx := files.New(*config)

		// this is not valid... since dropbox doesn't have fake directories but real ones!!!
		container, prefix := containerutils.GetContainerAndBlobPrefix(&sourceContainer)
		log.Debugf("DB prefix %s", prefix)
		log.Debugf("DB container %s", container.Name)

		var dirArg string
		if prefix != "" {
			dirArg = fmt.Sprintf("%s/%s", container.Name, prefix)
		} else {
			dirArg = "/" + container.Name
		}

		log.Debugf("DB dirarg %s", dirArg)

		arg := files.NewListFolderArg(dirArg)
		arg.Recursive = true

		res, err := dbx.ListFolder(arg)
		if err != nil {
			log.Fatalf("Dropbox::GetRootContainer error %s", err)
		}

		containerClone := sourceContainer
		processEntries(res, dirArg, &containerClone)

		log.Debugf("populate 1")
		blobChannel <- containerClone

		for res.HasMore {
			arg := files.NewListFolderContinueArg(res.Cursor)

			res, err = dbx.ListFolderContinue(arg)
			if err != nil {
				return err
			}
			containerClone := sourceContainer
			processEntries(res, dirArg, &containerClone)
			log.Debugf("populate 2")
			blobChannel <- containerClone
		}

		log.Debugf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
	*/

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

// GetContainerContentsOverChannel given a URL (ending in /) returns all the contents of the container over a channel
// This returns a COPY of the original source container but has been populated with *some* of the blobs/subcontainers in it.
func (dh *DropboxHandler) GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error) {

	log.Debugf("GetSpecificSimpleContainer url %s", URL)
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
			log.Debugf("ListFolderContinue err: %s", err)
			return nil, err
		}

		processEntries(res, dirArg, &container)
	}

	// get the container we're actually after.
	wantedContainer, err := filterContainer(&container, dirArg)
	if err != nil {
		log.Debugf("filterContainer returned error %s", err)
		return nil, err
	}

	log.Debugf("Dropbox::GetSpecificSimpleContainer returns container %s", wantedContainer.Name)
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
				log.Debugf("comparing against %s", childContainer.Name)
				if strings.ToLower(childContainer.Name) == strings.ToLower(dir) {
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

// populateSimpleContainer takes a list of Azure blobs and breaks them into virtual directories (SimpleContainers) and
// SimpleBlob trees.
//
// vdir1/vdir2/blob1
// vdir1/blob2
// vdir1/vdir3/blob3
// blob4

/*
func (dh *DropboxHandler) populateSimpleContainer(results *ListFolderResult, container *models.SimpleContainer) {

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


		}
	}

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
				subContainer := dh.getSubContainer(currentContainer, segment)

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
			b.URL = ""                  // unsure of URL
			currentContainer.BlobSlice = append(currentContainer.BlobSlice, &b)
			currentContainer.Populated = true

		}
	}
	container.Populated = true
}
*/

// getSubContainer gets an existing subcontainer with parent of container and name of segment.
// otherwise it creates it, adds it to the parent container and returns the new one.
func (dh *DropboxHandler) getSubContainer(container *models.SimpleContainer, segment string) *models.SimpleContainer {

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
	log.Debugf("processEntries sourceContainer %s", rootContainer.Name)
	for _, i := range results.Entries {
		switch f := i.(type) {
		case *files.FileMetadata:
			log.Debugf("DB is file %s", f.PathDisplay)
			blob := models.SimpleBlob{}
			blob.Name = f.Name
			//blob.URL = fmt.Sprintf("https://www.dropbox.com%s", f.PathDisplay) // NOT A REAL URL.... do we need it?
			blob.URL = f.PathDisplay // NOT A REAL URL.... do we need it?
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

	dbx := files.New(*config)
	arg := files.NewDownloadArg(blob.URL)
	log.Debugf("DB URL to download %s", blob.URL)
	res, contents, err := dbx.Download(arg)

	//if err != nil {
	//		log.Errorf("DB Cannot download blob %s, %s", blob.URL, err)
	//		return err
	//	}

	log.Debugf("res %s", res)
	log.Debugf("contents %s", contents)

	err = blobutils.ReadBlob(contents, blob, dh.cacheToDisk, dh.cacheLocation)
	if err != nil {
		log.Errorf("Error reading Dropbox blob %s", err)
		return err
	}

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
