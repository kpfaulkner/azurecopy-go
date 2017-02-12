package utils

import (
	"azurecopy/azurecopy/handlers"
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/misc"
	"io"
	"os"

	log "github.com/Sirupsen/logrus"
)

// GetHandler gets the appropriate handler for the cloudtype.
// Should I be doing this another way?
func GetHandler(cloudType models.CloudType, isSource bool, config misc.CloudConfig, cacheToDisk bool, isEmulator bool) handlers.CloudHandlerInterface {
	switch cloudType {
	case models.Azure:

		accountName, accountKey := GetAzureCredentials(isSource, config)

		log.Debug("Got Azure Handler")
		ah, _ := handlers.NewAzureHandler(accountName, accountKey, isSource, cacheToDisk, isEmulator)
		return ah

	case models.Filesystem:
		log.Debug("Got Filesystem Handler")
		var URL string
		if isSource {
			URL = config.Configuration[misc.Source]
		} else {
			URL = config.Configuration[misc.Dest]
		}
		fh, _ := handlers.NewFilesystemHandler(URL, isSource) // default path?
		return fh

	case models.S3:
		log.Debug("Got S3 Handler")
		accessID, accessSecret, region := getS3Credentials(isSource, config)

		sh, _ := handlers.NewS3Handler(accessID, accessSecret, region, isSource, true)
		return sh

	case models.DropBox:
		log.Debug("Got Dropbox Handler")
		dh, _ := handlers.NewDropboxHandler(isSource, true)
		return dh
	}

	return nil
}

func GetAzureCredentials(isSource bool, config misc.CloudConfig) (accountName string, accountKey string) {
	if isSource {
		accountName = config.Configuration[misc.AzureSourceAccountName]
		accountKey = config.Configuration[misc.AzureSourceAccountKey]
	} else {
		accountName = config.Configuration[misc.AzureDestAccountName]
		accountKey = config.Configuration[misc.AzureDestAccountKey]
	}

	if accountName == "" || accountKey == "" {
		accountName = config.Configuration[misc.AzureDefaultAccountName]
		accountKey = config.Configuration[misc.AzureDefaultAccountKey]
	}

	return accountName, accountKey
}

func getS3Credentials(isSource bool, config misc.CloudConfig) (accessID string, accessSecret string, region string) {
	if isSource {
		accessID = config.Configuration[misc.S3SourceAccessID]
		accessSecret = config.Configuration[misc.S3SourceAccessSecret]
		region = config.Configuration[misc.S3SourceRegion]
	} else {
		accessID = config.Configuration[misc.S3DestAccessID]
		accessSecret = config.Configuration[misc.S3DestAccessSecret]
		region = config.Configuration[misc.S3DestRegion]
	}

	if accessID == "" || accessSecret == "" {
		accessID = config.Configuration[misc.S3DefaultAccessID]
		accessSecret = config.Configuration[misc.S3DefaultAccessSecret]
		region = config.Configuration[misc.S3DefaultRegion]
	}

	return accessID, accessSecret, region
}

func readBlob(reader io.ReadCloser, blob *models.SimpleBlob, cacheToDisk bool, cacheLocation string) error {
	// file stream for cache.
	var cacheFile *os.File

	// populate this to disk.
	if cacheToDisk {

		cacheName := misc.GenerateCacheName(blob.BlobCloudName)
		blob.DataCachedAtPath = cacheLocation + "/" + cacheName
		log.Debugf("cache location is %s", blob.DataCachedAtPath)
		cacheFile, err := os.OpenFile(blob.DataCachedAtPath, os.O_WRONLY|os.O_CREATE, 0666)
		defer cacheFile.Close()

		if err != nil {
			log.Fatalf("Populate blob %s", err)
			return err
		}
	} else {
		blob.DataInMemory = []byte{}
	}

	// 100k buffer... way too small?
	buffer := make([]byte, 1024*100)
	finishedProcessing := false
	for finishedProcessing == false {
		numBytesRead, err := reader.Read(buffer)
		if err != nil {
			finishedProcessing = true
		}

		if numBytesRead <= 0 {
			finishedProcessing = true
			continue
		}

		// if we're caching, write to a file.
		if cacheToDisk {
			_, err := cacheFile.Write(buffer[:numBytesRead])
			if err != nil {
				log.Fatal(err)
				return err
			}
		} else {

			// needs to go into a byte array. How do we expand a slice again?
			blob.DataInMemory = append(blob.DataInMemory, buffer[:numBytesRead]...)
		}
	}

	return nil
}
