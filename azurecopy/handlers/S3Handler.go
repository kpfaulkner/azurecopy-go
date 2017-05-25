package handlers

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/containerutils"
	"azurecopy/azurecopy/utils/misc"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Handler struct {
	s3Client *s3.S3

	// determine if we're caching the blob to disk during copy operations.
	// or if we're keeping it in memory
	cacheToDisk   bool
	cacheLocation string

	// is this handler for the source or dest?
	IsSource bool
}

// NewS3Handler factory to create new one. Evil?
func NewS3Handler(accessID string, accessSecret string, region string, isSource bool, cacheToDisk bool) (*S3Handler, error) {

	sh := new(S3Handler)

	sh.cacheToDisk = cacheToDisk
	dir, err := ioutil.TempDir("", "azurecopy")
	if err != nil {
		log.Fatalf("Unable to create temp directory %s", err)
	}

	sh.cacheLocation = dir
	sh.IsSource = isSource

	creds := credentials.NewStaticCredentials(accessID, accessSecret, "")
	_, err = creds.Get()
	if err != nil {
		log.Fatalf("Bad S3 credentials: %s", err)
	}

	cfg := aws.NewConfig().WithRegion(region).WithCredentials(creds)

	log.Print(cfg)
	sh.s3Client = s3.New(session.New(), cfg)

	return sh, nil
}

// GetRootContainer gets root container of S3. Gets the list of buckets and THOSE are the immediate child containers here.
func (sh *S3Handler) GetRootContainer() models.SimpleContainer {
	result, err := sh.s3Client.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		log.Fatalf("Unable to get S3 buckets", err)
	}

	rootContainer := models.NewSimpleContainer()

	for _, bucket := range result.Buckets {
		sc := models.NewSimpleContainer()
		sc.Name = *bucket.Name
		sc.Origin = models.S3

		rootContainer.ContainerSlice = append(rootContainer.ContainerSlice, sc)
	}

	return *rootContainer
}

// BlobExists checks if blob exists
func (sh *S3Handler) BlobExists(container models.SimpleContainer, blobName string) (bool, error) {
	return false, nil
}

// convertURL converts from https://bucketname.s3.amazonaws.com/myblob to https://s3.amazonaws.com/bucketname/myblob format
func (sh *S3Handler) convertURL(URL string) string {

	// TODO(kpfaulkner) implement me!!!
	return URL

}

// populateSimpleContainer takes a list of Azure blobs and breaks them into virtual directories (SimpleContainers) and
// SimpleBlob trees.
//
// vdir1/vdir2/blob1
// vdir1/blob2
// vdir1/vdir3/blob3
// blob3
func (sh *S3Handler) populateSimpleContainer(s3Objects []*s3.Object, container *models.SimpleContainer, blobPrefix string) {

	log.Debugf("populateSimpleContainer original container %s", container.Name)
	for _, blob := range s3Objects {
		log.Debugf("populateSimpleContainer %s", *blob.Key)

		// if key ends in / then its just a fake directory.
		// do we even want to store that?
		// for now, skip it.

		if strings.HasSuffix(*blob.Key, "/") {
			// skip it.
			continue
		}

		prunedBlobName := *blob.Key
		if blobPrefix != "" {
			prunedBlobName = prunedBlobName[len(blobPrefix):]
		}

		log.Debugf("pruned blob name %s", prunedBlobName)

		// need to shorten name to remove the container name itself.
		// ie if the name of a blob if foo/bar.txt but we are currently in the "foo" container (fake vdir)
		// then we need to prune the container name from the DestName for the blob.
		sp := strings.Split(prunedBlobName, "/")

		// if no / then no subdirs etc. Just add as is.
		if len(sp) == 1 {
			b := models.SimpleBlob{}
			b.Name = prunedBlobName
			b.Origin = container.Origin
			b.ParentContainer = container
			b.BlobCloudName = *blob.Key
			b.URL = generateS3URL(*blob.Key, container.Name)
			// add to the blob slice within the container
			container.BlobSlice = append(container.BlobSlice, &b)
			log.Debugf("1 S3 blob %v", b)
		} else {

			currentContainer := container
			// if slashes, then split into chunks and create accordingly.
			// skip last one since thats the blob name.
			spShort := sp[0 : len(sp)-1]
			for _, segment := range spShort {

				// check if container already has a subcontainer with appropriate name
				subContainer := sh.getSubContainer(currentContainer, segment)
				if subContainer != nil {
					// then we have a blob so add it to currentContainer
					currentContainer = subContainer
				}
			}

			b := models.SimpleBlob{}
			b.Name = sp[len(sp)-1]
			b.Origin = container.Origin
			b.ParentContainer = container
			b.BlobCloudName = *blob.Key // cloud specific name... ie the REAL name.
			b.URL = generateS3URL(*blob.Key, container.Name)
			currentContainer.BlobSlice = append(currentContainer.BlobSlice, &b)
			currentContainer.Populated = true

			log.Debugf("2 S3 blob name %s", b.Name)

		}
	}
	container.Populated = true
}

// need a more solid way to generate this.
func generateS3URL(key string, containerName string) string {
	return fmt.Sprintf("https://%s.s3.amazonaws.com/%s", containerName, key)
}

// getSubContainer gets an existing subcontainer with parent of container and name of segment.
// otherwise it creates it, adds it to the parent container and returns the new one.
func (sh *S3Handler) getSubContainer(container *models.SimpleContainer, segment string) *models.SimpleContainer {

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

// GetContainerContentsOverChannel given a URL (ending in /) returns all the contents of the container over a channel
// This returns a COPY of the original source container but has been populated with *some* of the blobs/subcontainers in it.
func (sh *S3Handler) GetContainerContentsOverChannel(sourceContainer models.SimpleContainer, blobChannel chan models.SimpleContainer) error {

	log.Debugf("GetContainerContentsOverChannel source container %s", sourceContainer.Name)
	s3Container, blobPrefix := containerutils.GetContainerAndBlobPrefix(&sourceContainer)

	log.Debugf("s3 container %s BlobPrefix %s", s3Container, blobPrefix)
	defer close(blobChannel)

	params := s3.ListObjectsV2Input{
		Bucket: &s3Container.Name,
		Prefix: &blobPrefix,
	}

	err := sh.s3Client.ListObjectsV2Pages(&params,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			// copy of container, dont want to send back ever growing container via the channel.
			containerClone := sourceContainer
			sh.populateSimpleContainer(page.Contents, &containerClone, blobPrefix)
			blobChannel <- containerClone

			return !lastPage
		})

	if err != nil {
		return err
	}

	return nil
}

func (sh *S3Handler) getS3Bucket(containerName string) (*models.SimpleContainer, error) {

	rootContainer := sh.GetRootContainer()

	for _, container := range rootContainer.ContainerSlice {
		if container.Name == containerName {
			return container, nil
		}
	}

	return nil, errors.New("Unable to find container")

}

func (sh *S3Handler) generateSubContainers(s3Container *models.SimpleContainer, blobPrefix string) (*models.SimpleContainer, *models.SimpleContainer) {

	var containerToReturn *models.SimpleContainer
	var lastContainer *models.SimpleContainer
	doneFirst := false

	// strip off last /
	if len(blobPrefix) > 0 {
		blobPrefix = blobPrefix[:len(blobPrefix)-1]
		sp := strings.Split(blobPrefix, "/")

		for _, segment := range sp {
			container := models.NewSimpleContainer()
			container.Name = segment
			if !doneFirst {
				container.ParentContainer = s3Container
				containerToReturn = container
				doneFirst = true
			} else {
				container.ParentContainer = lastContainer
				lastContainer.ContainerSlice = append(lastContainer.ContainerSlice, container)
			}

			lastContainer = container
		}
	} else {

		// just return existing container (lastContainer) and no subcontainer (containerToReturn)
		containerToReturn = nil
		lastContainer = s3Container
	}

	return containerToReturn, lastContainer
}

// GetSpecificSimpleContainer for S3 will be the bucket.
// Conversion from https://bucketname.s3.amazonaws.com/myblob to https://s3.amazonaws.com/bucketname/myblob is done first.
func (sh *S3Handler) GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error) {

	URL = sh.convertURL(URL)

	lastChar := URL[len(URL)-1:]
	// MUST be a better way to get the last character.
	if lastChar != "/" {
		return nil, errors.New("Needs to end with a /")
	}

	containerName, blobPrefix, err := sh.validateURL(URL)
	if err != nil {
		log.Fatal("GetSpecificSimpleContainer err", err)
	}

	log.Debugf("S3 blobprefix %s", blobPrefix)
	container, err := sh.getS3Bucket(containerName)
	if err != nil {
		log.Fatal(err)
	}

	subContainer, lastContainer := sh.generateSubContainers(container, blobPrefix)

	if subContainer != nil {
		container.ContainerSlice = append(container.ContainerSlice, subContainer)
	}

	fmt.Printf("S3 specific container %v\n", lastContainer)
	fmt.Printf("S3 specific container name %s\n", lastContainer.Name)
	return lastContainer, nil
}

// validateURL returns container (bucket) Name, blob Name and error
// passes real URL such as https://s3.amazonaws.com/mybucket/myfileprefix/
func (sh *S3Handler) validateURL(URL string) (string, string, error) {

	lowerURL := strings.ToLower(URL)

	// ugly, do this properly!!! TODO(kpfaulkner)
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

	containerName := sp[1]
	blobName := strings.Join(sp[2:], "/")

	return containerName, blobName, nil
}

// GetSpecificSimpleBlob given a URL (NOT ending in /) then get the SIMPLE blob that represents it.
// The DestName will be the last element of the URL, whether it's a real blobname or not.
// eg.  https://...../mycontainer/vdir1/vdir2/blobname    will return a DestName of "blobname" even though strictly
// speaking the true blobname is "vdir1/vdir2/blobname".
// Will revisit this if it causes a problem.
func (sh *S3Handler) GetSpecificSimpleBlob(URL string) (*models.SimpleBlob, error) {
	// MUST be a better way to get the last character.
	if URL[len(URL)-2:len(URL)-1] == "/" {
		return nil, errors.New("Cannot end with a /")
	}

	containerName, blobName, err := sh.validateURL(URL)
	if err != nil {
		log.Fatal("GetSpecificSimpleContainer err", err)
	}

	// get parent container (ie this will be the real S3 bucket)
	parentContainer, err := sh.getS3Bucket(containerName)
	if err != nil {
		return nil, err
	}

	b := models.SimpleBlob{}
	b.Name = blobName
	b.Origin = models.S3
	b.ParentContainer = parentContainer
	b.BlobCloudName = blobName
	return &b, nil
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

// generateS3ContainerName gets the REAL Azure container name for the simpleBlob
func (sh *S3Handler) generateS3ContainerName(blob models.SimpleBlob) string {
	currentContainer := blob.ParentContainer

	for currentContainer.ParentContainer != nil {
		currentContainer = currentContainer.ParentContainer
	}
	return currentContainer.Name
}

// PopulateBlob. Used to read a blob IFF we already have a reference to it.
func (sh *S3Handler) PopulateBlob(blob *models.SimpleBlob) error {

	containerName := sh.generateS3ContainerName(*blob)

	req := &s3.GetObjectInput{
		Bucket: &containerName,
		Key:    aws.String(blob.BlobCloudName),
	}

	objectData, err := sh.s3Client.GetObject(req)
	if err != nil {
		log.Error(err)
		return err
	}

	defer objectData.Body.Close()

	// file stream for cache.
	var cacheFile *os.File

	// populate this to disk.
	if sh.cacheToDisk {

		cacheName := misc.GenerateCacheName(containerName + blob.BlobCloudName)
		blob.DataCachedAtPath = sh.cacheLocation + "/" + cacheName

		cacheFile, err = os.OpenFile(blob.DataCachedAtPath, os.O_WRONLY|os.O_CREATE, 0666)
		defer cacheFile.Close()

		if err != nil {
			log.Fatal(err)
			return err
		}
	} else {
		blob.DataInMemory = []byte{}
	}

	// 100k buffer... way too small?
	buffer := make([]byte, 1024*100)
	numBytesRead := 0

	finishedProcessing := false
	for finishedProcessing == false {
		numBytesRead, err = objectData.Body.Read(buffer)
		if err != nil {
			finishedProcessing = true
		}

		if numBytesRead <= 0 {
			finishedProcessing = true
			continue
		}

		// if we're caching, write to a file.
		if sh.cacheToDisk {
			_, err = cacheFile.Write(buffer[:numBytesRead])
			if err != nil {
				log.Fatal(err)
				return err
			}
		} else {

			// needs to go into a byte array. How do we expand a slice again?
			blob.DataInMemory = append(blob.DataInMemory, buffer[:numBytesRead]...)
		}
	}

	if cacheFile != nil {
		log.Debugf("manually closing file %s", blob.DataCachedAtPath)
		cacheFile.Close()
	} else {
		log.Debug("no manual closing needed")
	}

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

	var err error
	if sh.cacheToDisk {
		err = sh.writeBlobFromCache(destContainer, sourceBlob)
	} else {
		err = sh.writeBlobFromMemory(destContainer, sourceBlob)
	}

	if err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}

func (sh *S3Handler) getContainerAndBlobNames(destContainer *models.SimpleContainer, sourceBlobName string) (string, string) {

	container, blobPrefix := containerutils.GetContainerAndBlobPrefix(destContainer)
	containerName := container.Name

	var blobName string

	if blobPrefix != "" {
		blobName = blobPrefix + "/" + sourceBlobName
	} else {
		blobName = sourceBlobName
	}

	return containerName, blobName
}

// writeBlobFromCache.. read the cache file and pass the byte slice onto the real writer.
func (sh *S3Handler) writeBlobFromCache(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {
	containerName, blobName := sh.getContainerAndBlobNames(destContainer, sourceBlob.Name)

	// file stream for cache.
	var cacheFile *os.File

	// need to get cache dir from somewhere!
	cacheFile, err := os.OpenFile(sourceBlob.DataCachedAtPath, os.O_RDONLY, 0)
	if err != nil {
		log.Errorf("Unable to open cache file %s", sourceBlob.DataCachedAtPath)
		return err
	}
	defer cacheFile.Close()

	params := &s3.PutObjectInput{
		Bucket: aws.String(containerName),
		Key:    aws.String(blobName),
		Body:   cacheFile,
	}
	_, err = sh.s3Client.PutObject(params)
	if err != nil {
		log.Errorf("Unable to upload %s", blobName)
		return err
	}

	return nil
}

func (sh *S3Handler) writeBlobFromMemory(destContainer *models.SimpleContainer, sourceBlob *models.SimpleBlob) error {
	containerName, blobName := sh.getContainerAndBlobNames(destContainer, sourceBlob.Name)

	fileBytes := bytes.NewReader(sourceBlob.DataInMemory) // convert to io.ReadSeeker type

	params := &s3.PutObjectInput{
		Bucket: aws.String(containerName),
		Key:    aws.String(blobName),
		Body:   fileBytes,
	}
	_, err := sh.s3Client.PutObject(params)
	if err != nil {
		log.Errorf("Unable to upload %s", blobName)
		return err
	}

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
// For S3 only the children of the root node can be a real azure container. Everything else
// is a blob or a blob pretending to have vdirs.
func (sh *S3Handler) GetContainerContents(container *models.SimpleContainer) error {
	s3Container, blobPrefix := containerutils.GetContainerAndBlobPrefix(container)

	params := s3.ListObjectsV2Input{
		Bucket: &s3Container.Name,
		Prefix: &blobPrefix,
	}

	// slice of every object. This might get a tad large.
	// do we need to pass in pieces over channels?
	blobSlice := []*s3.Object{}

	err := sh.s3Client.ListObjectsV2Pages(&params,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			// copy of container, dont want to send back ever growing container via the channel.

			// variadic functions...   look it up. :)
			blobSlice = append(blobSlice, page.Contents...)

			// return entire container via channel.
			return !lastPage
		})

	if err != nil {
		return err
	}

	sh.populateSimpleContainer(blobSlice, container, blobPrefix)

	return nil
}

/* presign URL code.....  use it eventually.
 */

func (sh *S3Handler) GeneratePresignedURL(blob *models.SimpleBlob) (string, error) {

	log.Debugf("S3:GeneratePresignedURL")
	s3Container, _ := containerutils.GetContainerAndBlobPrefix(blob.ParentContainer)

	r, _ := sh.s3Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s3Container.Name),
		Key:    aws.String(blob.BlobCloudName),
	})

	//r.HTTPRequest.Header.Set("content-type", "application/octet-stream")
	// r.HTTPRequest.Header.Set("Content-MD5", checksum)
	url, err := r.Presign(15 * time.Minute)
	if err != nil {
		log.Error("error presigning request", err)
		return "", err
	}

	log.Debugf("presigning with %s and %s", s3Container.Name, blob.BlobCloudName)
	log.Debugf("presigned URL is %s", url)
	return url, nil
}
