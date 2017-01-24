package handlers

import (
	"azurecopy/azurecopy/models"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	log "github.com/Sirupsen/logrus"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/oauth2"
)

type DropboxHandler struct {

	// determine if we're caching the blob to disk during copy operations.
	// or if we're keeping it in memory
	cacheToDisk   bool
	cacheLocation string

	// is this handler for the source or dest?
	IsSource bool
}

// Map of map of strings
// For each domain, we want to save different tokens depending on the
// command type: personal, team access and team manage
type TokenMap map[string]map[string]string

var config dropbox.Config

const (
	configFileName = "azurecopyauth.json"
	appKey         = "XXX"
	appSecret      = "XXX"
	dropboxScheme  = "dropbox"

	tokenPersonal   = "personal"
	tokenTeamAccess = "teamAccess"
	tokenTeamManage = "teamManage"
)

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

	setupConnection()
	return dh, nil
}

func setupConnection() {
	conf := oauth2.Config{
		ClientID:     appKey,
		ClientSecret: appSecret,
		Endpoint:     dropbox.OAuthEndpoint(""),
	}

	dir, err := homedir.Dir()
	if err != nil {
		return
	}
	filePath := path.Join(dir, ".config", "azurecopy", configFileName)
	tokType := tokenPersonal

	tokenMap, err := readTokens(filePath)
	if tokenMap == nil {
		tokenMap = make(TokenMap)
	}
	domain := ""

	if tokenMap[domain] == nil {
		tokenMap[domain] = make(map[string]string)
	}
	tokens := tokenMap[domain]

	if err != nil || tokens[tokType] == "" {
		fmt.Printf("1. Go to %v\n", conf.AuthCodeURL("state"))
		fmt.Printf("2. Click \"Allow\" (you might have to log in first).\n")
		fmt.Printf("3. Copy the authorization code.\n")
		fmt.Printf("Enter the authorization code here: ")

		var code string
		if _, err = fmt.Scan(&code); err != nil {
			return
		}
		var token *oauth2.Token
		token, err = conf.Exchange(oauth2.NoContext, code)
		if err != nil {
			return
		}
		tokens[tokType] = token.AccessToken
		writeTokens(filePath, tokenMap)
	} else {
		log.Debugf("Already have Dropbox token")
	}

	config = dropbox.Config{tokens[tokType], true, "", domain}
}

func readTokens(filePath string) (TokenMap, error) {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var tokens TokenMap
	if json.Unmarshal(b, &tokens) != nil {
		return nil, err
	}

	return tokens, nil
}

func writeTokens(filePath string, tokens TokenMap) {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Doesn't exist; lets create it
		err = os.MkdirAll(filepath.Dir(filePath), 0700)
		if err != nil {
			return
		}
	}

	// At this point, file must exist. Lets (over)write it.
	b, err := json.Marshal(tokens)
	if err != nil {
		return
	}
	if err = ioutil.WriteFile(filePath, b, 0600); err != nil {
		return
	}
}

// GetRootContainer gets root container of S3. Gets the list of buckets and THOSE are the immediate child containers here.
func (dh *DropboxHandler) GetRootContainer() models.SimpleContainer {
	container := models.SimpleContainer{}
	dbx := files.New(config)

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

	return nil
}

// GetSpecificSimpleContainer for S3 will be the bucket.
// Conversion from https://bucketname.s3.amazonaws.com/myblob to https://s3.amazonaws.com/bucketname/myblob is done first.
func (dh *DropboxHandler) GetSpecificSimpleContainer(URL string) (*models.SimpleContainer, error) {
	dbx := files.New(config)

	arg := files.NewListFolderArg("")

	res, err := dbx.ListFolder(arg)
	if err != nil {
		log.Fatalf("Dropbox::GetRootContainer error %s", err)
	}

	log.Debugf("results are %s", res)

	container := models.SimpleContainer{}

	for _, i := range res.Entries {
		log.Debugf("res %s", i)
		switch f := i.(type) {
		case *files.FileMetadata:
			blob := models.SimpleBlob{}
			blob.Name = f.Name
			blob.URL = fmt.Sprintf("https://www.dropbox.com/%s", f.Name) // NOT A REAL URL.... do we need it?
			blob.Origin = models.DropBox
			blob.ParentContainer = &container
			container.BlobSlice = append(container.BlobSlice, &blob)
			log.Debugf("FILE %s", f.Name)
		case *files.FolderMetadata:
			c := models.SimpleContainer{}
			c.Name = f.Name
			c.ParentContainer = &container
			c.URL = fmt.Sprintf("https://www.dropbox.com/%s", f.Name) // NOT A REAL URL.... do we need it?
			c.Origin = models.DropBox
			container.ContainerSlice = append(container.ContainerSlice, &c)
			log.Debugf("DIR %s", f.Name)
		}
	}

	return &container, nil
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
