package helpers

import (
	"azure-storage-go"
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/containerutils"

	log "github.com/Sirupsen/logrus"
)

type AzureHelper struct {
	client storage.BlobStorageClient
}

func NewAzureHelper(accountName string, accountKey string) *AzureHelper {

	ah := new(AzureHelper)

	client, err := storage.NewBasicClient(accountName, accountKey)
	if err != nil {
		log.Fatalf("NewAzureHelper cannot generate Azure Storage client", err)
	}

	ah.client = client.GetBlobService()
	return ah
}

// DoCopyBlobUsingAzureCopyBlobFlag copy using Azure CopyBlob flag.
// Have to create a new instance of the storage client. I can't get it out of AzureHandler since
// we get that via an interface, and obviously not all handlers will have Azure clients.
// TODO(kpfaulkner) revisit and find a better way, but new client for now isn't completely terrible.
func (ah *AzureHelper) DoCopyBlobUsingAzureCopyBlobFlag(url string, destContainer *models.SimpleContainer, destBlobName string) error {

	// need to get real azure container but I *think* destBlobName has already been correctly converted.
	// need to check that! TODO(kpfaulkner)

	container, _ := containerutils.GetContainerAndBlobPrefix(destContainer)

	log.Debugf("CopyBlob: source %s : dest container %s : blobname %s", url, container.Name, destBlobName)
	err := ah.client.CopyBlob(container.Name, destBlobName, url)
	if err != nil {
		log.Errorf("Unable to copy %s %s", url, err)
	}
	return nil
}
