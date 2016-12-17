package main

import (
	"azurecopy/azurecopy"
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/misc"
	"flag"

	log "github.com/Sirupsen/logrus"
)

/*
func FSToAzure() {
	ac := azurecopy.NewAzureCopy(true)

	rootContainer := ac.GetRootContainer(models.Filesystem)

	for _, c := range rootContainer.ContainerSlice {
		if c.Name == "img" {
			ac.GetContainerContents(c)

			blob, err := c.GetBlob("F9.JPG")
			if err != nil {
				log.Fatal(err)
			}
			azureRootContainer := ac.GetRootContainer(models.Azure)

			ac.ReadBlob(blob)

			tempContainer, err := azureRootContainer.GetContainer("temp")
			if err != nil {
				log.Fatal("GetContainer ", err)
			}

			ac.WriteBlob(tempContainer, blob)
		}
	}

}

func messingABout() {
	ac := azurecopy.NewAzureCopy(true)

	azureRootContainer := ac.GetRootContainer(models.Azure)

	for _, c := range azureRootContainer.ContainerSlice {
		if c.Name == "temp" {
			ac.GetContainerContents(c)

			blob, err := c.GetBlob("F9.JPG")
			if err != nil {
				log.Fatal(err)
			}
			rootContainer := ac.GetRootContainer(models.Filesystem)

			ac.ReadBlob(blob)

			tempContainer, err := rootContainer.GetContainer("temp")
			if err != nil {
				log.Fatal("GetContainer ", err)
			}

			ac.WriteBlob(tempContainer, blob)
		}
	}

}
*/

func generateSpace(c int) string {
	s := ""
	for i := 0; i < c; i++ {
		s = s + " "
	}

	return s
}

func printContainer(container *models.SimpleContainer, depth int) {
	s := generateSpace(depth)

	log.Printf("%scontainer: %s", s, container.Name)

	depth = depth + 2
	s = generateSpace(depth)

	for _, b := range container.BlobSlice {
		log.Printf("%sblob: %s", s, b.Name)
	}

	for _, c := range container.ContainerSlice {
		printContainer(c, depth)
	}

}

func setupConfiguration() *misc.CloudConfig {
	config := misc.NewCloudConfig()

	var source = flag.String("source", "", "Source URL")
	var dest = flag.String("dest", "", "Destination URL")
	var debug = flag.Bool("debug", false, "Debug output")

	var azureDefaultAccountName = flag.String("AzureDefaultAccountName", "", "Default Azure Account Name")
	var azureDefaultAccountKey = flag.String("AzureDefaultAccountKey", "", "Default Azure Account Key")
	var azureSourceAccountName = flag.String("AzureSourceAccountName", "", "Source Azure Account Name")
	var azureSourceAccountKey = flag.String("AzureSourceAccountKey", "", "Source Azure Account Key")
	var azureDestAccountName = flag.String("AzureDestAccountName", "", "Destination Azure Account Name")
	var azureDestAccountKey = flag.String("AzureDestAccountKey", "", "Destination Azure Account Key")

	var s3DefaultAccessID = flag.String("S3DefaultAccessID", "", "Default S3 Access ID")
	var s3DefaultAccessSecret = flag.String("S3DefaultAccessSecret", "", "Default S3 Access Secret")
	var s3SourceAccessID = flag.String("S3SourceAccessID", "", "Source S3 Access ID")
	var s3SourceAccessSecret = flag.String("S3SourceAccessSecret", "", "Source S3 Access Secret")
	var s3DestAccessID = flag.String("S3DestAccessID", "", "Destination S3 Access ID")
	var s3DestAccessSecret = flag.String("S3DestAccessSecret", "", "Destination S3 Access Secret")

	flag.Parse()

	config.Configuration[misc.Source] = *source
	config.Configuration[misc.Dest] = *dest
	config.Debug = *debug

	config.Configuration[misc.AzureDefaultAccountName] = *azureDefaultAccountName
	config.Configuration[misc.AzureDefaultAccountKey] = *azureDefaultAccountKey
	config.Configuration[misc.AzureSourceAccountName] = *azureSourceAccountName
	config.Configuration[misc.AzureSourceAccountKey] = *azureSourceAccountKey
	config.Configuration[misc.AzureDestAccountName] = *azureDestAccountName
	config.Configuration[misc.AzureDestAccountKey] = *azureDestAccountKey

	config.Configuration[misc.S3DefaultAccessID] = *s3DefaultAccessID
	config.Configuration[misc.S3DefaultAccessSecret] = *s3DefaultAccessSecret
	config.Configuration[misc.S3SourceAccessID] = *s3SourceAccessID
	config.Configuration[misc.S3SourceAccessSecret] = *s3SourceAccessSecret
	config.Configuration[misc.S3DestAccessID] = *s3DestAccessID
	config.Configuration[misc.S3DestAccessSecret] = *s3DestAccessSecret

	return config
}

// "so it begins"
func main() {

	config := setupConfiguration()

	if !config.Debug {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(log.DebugLevel)
	}

	ac := azurecopy.NewAzureCopy(*config)
	err := ac.CopyBlobByURL()
	if err != nil {
		log.Fatal(err)
	}

}
