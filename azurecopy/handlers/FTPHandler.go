package handlers

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/containerutils"
	"azurecopy/azurecopy/utils/misc"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"
	"errors"
	"os"
	"bytes"
	"time"

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

	// determine if we're caching the blob to disk during copy operations.
	// or if we're keeping it in memory
	cacheToDisk   bool
	cacheLocation string

}

// NewFTPHandler factory to create new one. Evil?
func NewFTPHandler(address string, username string, password string, isSource bool, cacheToDisk bool) (*FTPHandler, error) {

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
	fh.cacheToDisk = cacheToDisk

	dir, err := ioutil.TempDir("", "azurecopy")
	if err != nil {
		log.Fatalf("Unable to create temp directory %s", err)
	}

	fh.cacheLocation = dir

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

// get base path and container name
// path will be something like myftp.com/dir1/dir2/mydir3
// will return /dir1/dir2  and /mydir3/
func getFTPContainerNameFromURL(url string) (string, string) {

	log.Debugf("rootContainerPath %s", url)
	if url != "" {
		var sp = strings.Split(url, "/")
		l := len(sp)

		log.Debugf("sp is %s", sp)
		genPath := strings.Join(sp[:l-2], "/") + "/"
		container := sp[l-2]

		return genPath, container
	}

	// wasn't passed, so return nada
	return "", ""
}

// GetSpecificSimpleContainer given a URL (ending in /) then get the SIMPLE container that represents it.
// does not have to have all blobs populated in it. Those can be retrieved later via GetContainerContentsOverChannel
// This is up to specific handlers. Currently (for example). For FTP if the url is myftpsite.com/dir1/dir2/dir3/ then it
// will return a SimpleContainer representing dir3 with all its contents.6
func (fh *FTPHandler) GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error) {
	if URL != "" {

		// check if its a container.
		if isContainer(URL) {

			var sp= strings.Split(URL, "/")
			parentContainer := models.NewSimpleContainer()
			parentContainer.IsRootContainer = true
			parentContainer.Origin = models.FTP
			currentContainer := parentContainer
			for _, segment := range sp[1:] {
				container := models.NewSimpleContainer()
				container.URL = URL
				container.Origin = models.FTP
				container.Name = segment
				container.IsRootContainer = false
				container.ParentContainer = currentContainer
				currentContainer.ContainerSlice = append(currentContainer.ContainerSlice, container)

				currentContainer = container
				log.Debugf("segment is %s\n", segment)
			}

			return currentContainer, nil
		}
	}

	return nil, errors.New("URL cannot be empty")
}

// GetContainerContents populates the container (directory) with the next level contents
// currently wont do recursive.
func (fh *FTPHandler) GetContainerContents(sourceContainer *models.SimpleContainer) error {

	/*  WIP!!!
	fullPath := fh.generateFullPath(sourceContainer)

	entryList, err := fh.client.List( fullPath)

	// loop through until all directories done....
	done := false
	for done == false {
		// copy of container, dont want to send back ever growing container via the channel.
		containerClone := sourceContainer

		//azureContainer := ah.blobStorageClient.GetContainerReference(azureContainer.Name)
		blobListResponse, err := containerURL.ListBlobs( ctx, marker, storage.ListBlobsOptions{ Prefix: blobPrefix})
		if err != nil {
			log.Fatal("Error")
		}

		ah.populateSimpleContainer(blobListResponse, &containerClone, blobPrefix)

		// return entire container via channel.
		blobChannel <- containerClone

		// if marker, then keep going.
		if blobListResponse.NextMarker.NotDone() {
			marker = blobListResponse.NextMarker
		} else {
			done = true
		}
	}

	close(blobChannel)

	*/
	return nil

}

// GetContainerContentsOverChannel given a URL (ending in /) returns all the contents of the container over a channel
// GetContainerContentsOverChannel given a URL (ending in /) returns all the contents of the container over a channel
// This returns a COPY of the original source container but has been populated with *some* of the blobs/subcontainers in it.
// This is going to be inefficient from a memory allocation pov.
// Am still creating various structs that we strictly do not require for copying (all the tree structure etc) but this will
// at least help each cloud provider be consistent from a dev pov. Think it's worth the overhead. TODO(kpfaulkner) confirm :)
func (fh *FTPHandler) GetContainerContentsOverChannel(sourceContainer models.SimpleContainer, blobChannel chan models.SimpleContainer) error {

	defer close(blobChannel)
	// just do it in bulk for FS. Figure out later if its an issue.
	fh.GetContainerContents(&sourceContainer)
	blobChannel <- sourceContainer

	return nil
}

// GetSpecificSimpleBlob given a URL (NOT ending in /) then get the SIMPLE blob that represents it.
// The DestName will be the last element of the URL, whether it's a real blobname or not.
// eg.  https://...../mycontainer/vdir1/vdir2/blobname    will return a DestName of "blobname" even though strictly
// speaking the true blobname is "vdir1/vdir2/blobname".
// Will revisit this if it causes a problem.
func (fh *FTPHandler) GetSpecificSimpleBlob(URL string) (*models.SimpleBlob, error) {

}

// dupe of filesystem. Need to check if can just use single method instance.
func (fh *FTPHandler) generateFullPath(container *models.SimpleContainer) string {

	path := container.Name
	currentContainer := container.ParentContainer
	for currentContainer != nil {
		if currentContainer.Name != "" {
			path = filepath.Join(currentContainer.Name, path)
		}

		currentContainer = currentContainer.ParentContainer
	}

	fullPath := fh.basePath + path + "/"
	// if full path is rootContainerPath then we need to actually generate
	return fullPath
}

func reverseStringSlice( s []string ) []string{
	for i := len(s)/2-1; i >= 0; i-- {
		opp := len(s)-1-i
		s[i], s[opp] = s[opp], s[i]
	}

	return s
}
// generate complete path of blob
func (fh *FTPHandler) generateBlobFullPath(blob *models.SimpleBlob) string {

	nameElements := []string{}
	nameElements = append(nameElements, blob.DestName)

	currentContainer := blob.ParentContainer
	for currentContainer != nil {
		if currentContainer.Name != "" {
			nameElements = append(nameElements, currentContainer.Name)
		}
		currentContainer = currentContainer.ParentContainer
	}

	nameElements = reverseStringSlice( nameElements)
	fullPath := strings.Join(nameElements, "/")
	return fullPath
}


// Given a container and a blob name, read the blob.
func (fh *FTPHandler) ReadBlob(container models.SimpleContainer, blobName string) models.SimpleBlob {
	var blob models.SimpleBlob

	dirPath := fh.generateFullPath(&container)
	fullPath := filepath.Join(dirPath, blobName)

	// populate this to disk.
	if fh.cacheToDisk {

		cacheName := misc.GenerateCacheName(blob.BlobCloudName)
		blob.DataCachedAtPath = fh.cacheLocation + "/" + cacheName
		log.Debugf("azure blob %s cached at location %s", blob.BlobCloudName, blob.DataCachedAtPath)
	}

	r, err := fh.client.Retr(fullPath)
	if err != nil {
		log.Fatal(err)
	} else {

		if fh.cacheToDisk {
			// read directly into cached file
			cacheFile, err := os.OpenFile(blob.DataCachedAtPath, os.O_WRONLY|os.O_CREATE, 0666)
			defer cacheFile.Close()
			if err != nil {
				log.Fatalf("Populate blob %s", err)
			}
			_, err = io.Copy(cacheFile, r)
			blob.BlobInMemory = false

		} else {
			buf, err := ioutil.ReadAll(r)
			if err != nil {
				log.Fatal(err)
			}
			r.Close() // test we can close two times

			blob.DataInMemory = buf
			blob.BlobInMemory = true
		}
	}

	blob.DataCachedAtPath = fullPath
	blob.Name = blobName
	blob.ParentContainer = &container
	blob.Origin = container.Origin
	blob.URL = fullPath
	return blob
}

// Does blob exist
// question if error should be returned?
func (fh *FTPHandler) BlobExists(container models.SimpleContainer, blobName string) (bool, error) {
	dirPath := fh.generateFullPath(&container)
	fullPath := filepath.Join(dirPath, blobName)

	_, err := fh.client.FileSize( fullPath)
	if err != nil {
		return false, nil
	}

	return true, nil

}

// if we already have a reference to a SimpleBlob, then read it and populate it.
// ie we're populating our in process copy of the blob (ie reading it from the provider).
func (fh *FTPHandler) PopulateBlob(blob *models.SimpleBlob) error {
	fullPath := fh.generateBlobFullPath( blob)

	// populate this to disk.
	if fh.cacheToDisk {

		cacheName := misc.GenerateCacheName(blob.BlobCloudName)
		blob.DataCachedAtPath = fh.cacheLocation + "/" + cacheName
		log.Debugf("azure blob %s cached at location %s", blob.BlobCloudName, blob.DataCachedAtPath)
	}

	r, err := fh.client.Retr(fullPath)
	if err != nil {
		log.Fatal(err)
	} else {

		if fh.cacheToDisk {
			// read directly into cached file
			cacheFile, err := os.OpenFile(blob.DataCachedAtPath, os.O_WRONLY|os.O_CREATE, 0666)
			defer cacheFile.Close()
			if err != nil {
				log.Fatalf("Populate blob %s", err)
			}
			_, err = io.Copy(cacheFile, r)
			blob.BlobInMemory = false

		} else {
			buf, err := ioutil.ReadAll(r)
			if err != nil {
				log.Fatal(err)
			}
			r.Close() // test we can close two times

			blob.DataInMemory = buf
			blob.BlobInMemory = true
		}
	}

	blob.DataCachedAtPath = fullPath
	blob.URL = fullPath

	return nil
}

func (fh *FTPHandler) createSubDirectories(fullPath string) error {
	err := fh.client.MakeDir(fullPath)
	if err != nil {
		return err
	}

	return nil
}

// given a container and blob, write blob.
func (fh *FTPHandler) WriteBlob(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {
	blobName := sourceBlob.Name
	if blobName[0] == os.PathSeparator {
		blobName = blobName[1:]
	}

	fullPath := fh.generateFullPath(destContainer) + blobName

	// make sure subdirs are created.
	err := fh.createSubDirectories(fullPath)
	if err != nil {
		log.Fatal(err)
		return err
	}

	if !sourceBlob.BlobInMemory {
		// cached on disk.
		newFile, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE, 0)
		if err != nil {
			return err
		}
		err = fh.client.Stor(fullPath, newFile )
		if err != nil {
			return err
		}
	} else {
		// in memory.
		err = fh.client.Stor( fullPath,bytes.NewReader( sourceBlob.DataInMemory) )
		if err != nil {
			log.Fatal("Unable to upload file " + fullPath, err)
		}
	}

	return nil
}

// write a container (and subcontents) to the appropriate data store
func (fh *FTPHandler) WriteContainer(sourceContainer *models.SimpleContainer, destContainer *models.SimpleContainer) error {
return nil
}

// Gets a container. Populating the subtree? OR NOT? hmmmm
func (fh *FTPHandler)  GetContainer(containerName string) models.SimpleContainer {
	var container models.SimpleContainer

	return container
}

// generates presigned URL so Azure can access blob for CopyBlob flag operation.
func (fh *FTPHandler)  GeneratePresignedURL(blob *models.SimpleBlob) (string, error) {
	return "", nil

}
