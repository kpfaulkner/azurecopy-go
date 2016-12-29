package handlers

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/containerutils"
	"errors"
	"regexp"
	"strings"

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
	sh.cacheLocation = "c:/temp/cache/" // NFI... just making something up for now
	sh.IsSource = isSource

	creds := credentials.NewStaticCredentials(accessID, accessSecret, "")
	_, err := creds.Get()
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
// blob4
func (sh *S3Handler) populateSimpleContainer(s3Objects []*s3.Object, container *models.SimpleContainer) {

	for _, blob := range s3Objects {

		sp := strings.Split(*blob.Key, "/")

		// if no / then no subdirs etc. Just add as is.
		if len(sp) == 1 {
			b := models.SimpleBlob{}
			b.Name = *blob.Key
			b.Origin = container.Origin
			b.ParentContainer = container
			b.BlobCloudName = *blob.Key
			// add to the blob slice within the container
			container.BlobSlice = append(container.BlobSlice, &b)
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
			b.URL = ""                  // NFI ah.blobStorageClient.GetBlobURL(container.Name, blob.Name)
			currentContainer.BlobSlice = append(currentContainer.BlobSlice, &b)
			currentContainer.Populated = true
			log.Debugf("just added blob %s to container", b.Name, currentContainer.Name)

		}
	}
	container.Populated = true
}

// getSubContainer gets an existing subcontainer with parent of container and name of segment.
// otherwise it creates it, adds it to the parent container and returns the new one.
func (sh *S3Handler) getSubContainer(container *models.SimpleContainer, segment string) *models.SimpleContainer {

	log.Debugf("S3Handler::getSubContainer looking for %s", segment)

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
// This is going to be inefficient from a memory allocation pov.
// Am still creating various structs that we strictly do not require for copying (all the tree structure etc) but this will
// at least help each cloud provider be consistent from a dev pov. Think it's worth the overhead. TODO(kpfaulkner) confirm :)
func (sh *S3Handler) GetContainerContentsOverChannel(sourceContainer models.SimpleContainer, blobChannel chan models.SimpleContainer) error {
	s3Container, blobPrefix := containerutils.GetContainerAndBlobPrefix(&sourceContainer)

	log.Debugf("Blob Prefix %s", blobPrefix)
	defer close(blobChannel)

	params := s3.ListObjectsV2Input{
		Bucket: &s3Container.Name,
		Prefix: &blobPrefix,
	}

	err := sh.s3Client.ListObjectsV2Pages(&params,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			// copy of container, dont want to send back ever growing container via the channel.
			containerClone := *s3Container
			sh.populateSimpleContainer(page.Contents, &containerClone)
			// return entire container via channel.
			blobChannel <- containerClone

			return !lastPage
		})

	if err != nil {
		return err
	}

	return nil
}

func (sh *S3Handler) getS3Bucket(containerName string) (*models.SimpleContainer, error) {

	log.Debugf("getS3Bucket %s", containerName)
	rootContainer := sh.GetRootContainer()

	for _, container := range rootContainer.ContainerSlice {
		if container.Name == containerName {
			return container, nil
		}
	}

	return nil, errors.New("Unable to find container")

}

func (sh *S3Handler) generateSubContainers(s3Container *models.SimpleContainer, blobPrefix string) (*models.SimpleContainer, *models.SimpleContainer) {

	log.Debugf("generateSubContainers %s : prefix %s", s3Container.Name, blobPrefix)

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

	log.Debugf("GetSpecificSimpleContainer %s", URL)

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

	/*
			contents, err := sh.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
				Bucket: &containerName,
				Prefix: &blobPrefix,
			})

			if err != nil {
				log.Fatal("GetSpecificSimpleContainer err", err)
			}

		log.Printf("contents %s", contents)
	*/

	container, err := sh.getS3Bucket(containerName)
	if err != nil {
		log.Fatal(err)
	}

	log.Debugf("Container: %s blobPrefix: %s", container, blobPrefix)
	subContainer, lastContainer := sh.generateSubContainers(container, blobPrefix)

	if subContainer != nil {
		container.ContainerSlice = append(container.ContainerSlice, subContainer)
	}

	log.Debugf("GetSpecificSimpleContainer returning %s", lastContainer.Name)

	// return the "deepest" container.
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

	sh.populateSimpleContainer(blobSlice, container)

	return nil
}
