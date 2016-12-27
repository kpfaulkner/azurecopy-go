package handlers

import (
	"azurecopy/azurecopy/models"
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
func NewS3Handler(accessID string, accessSecret string, isSource bool, cacheToDisk bool) (*S3Handler, error) {
	sh := new(S3Handler)

	sh.cacheToDisk = cacheToDisk
	sh.cacheLocation = "c:/temp/cache/" // NFI... just making something up for now
	sh.IsSource = isSource

	creds := credentials.NewStaticCredentials(accessID, accessSecret, "")
	_, err := creds.Get()
	if err != nil {
		log.Fatalf("Bad S3 credentials: %s", err)
	}

	cfg := aws.NewConfig().WithRegion("us-west-1").WithCredentials(creds)

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

	contents, err := sh.s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: &containerName,
		Prefix: &blobPrefix,
	})

	if err != nil {
		log.Fatal("GetSpecificSimpleContainer err", err)
	}

	lastContainer := models.NewSimpleContainer()

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
// For Azure only the children of the root node can be a real azure container. Everything else
// is a blob or a blob pretending to have vdirs.
func (sh *S3Handler) GetContainerContents(container *models.SimpleContainer) {

}
