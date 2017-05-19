package blobutils

import (
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/misc"
	"io"
	"os"

	log "github.com/Sirupsen/logrus"
)

func ReadBlob(reader io.ReadCloser, blob *models.SimpleBlob, cacheToDisk bool, cacheLocation string) error {
	// file stream for cache.
	var cacheFile *os.File
	var err error

	// populate this to disk.
	if cacheToDisk {

		cacheName := misc.GenerateCacheName(blob.BlobCloudName)
		blob.DataCachedAtPath = cacheLocation + "/" + cacheName
		log.Debugf("cache location is %s", blob.DataCachedAtPath)
		cacheFile, err = os.OpenFile(blob.DataCachedAtPath, os.O_WRONLY|os.O_CREATE, 0666)
		//defer cacheFile.Close()

		if err != nil {
			log.Fatalf("Populate blob %s", err)
			return err
		}
	} else {
		blob.DataInMemory = []byte{}
	}

	log.Debugf("cachefile early is %s", cacheFile)
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

		//log.Debugf("bytes %s", buffer)
		log.Debugf("number of bytes read %d", numBytesRead)
		// if we're caching, write to a file.
		if cacheToDisk {
			_, err := cacheFile.Write(buffer[:numBytesRead])
			if err != nil {
				log.Debugf("cachefile %s", cacheFile)

				log.Fatalf("cache to disk fatal %s", err)
				return err
			}
		} else {

			// needs to go into a byte array. How do we expand a slice again?
			blob.DataInMemory = append(blob.DataInMemory, buffer[:numBytesRead]...)
		}
	}

	return nil
}
