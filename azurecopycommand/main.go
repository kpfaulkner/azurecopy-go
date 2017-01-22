package main

import (
	"azurecopy/azurecopy"
	"azurecopy/azurecopy/models"
	"azurecopy/azurecopy/utils/misc"
	"flag"
	"fmt"

	"os"

	log "github.com/Sirupsen/logrus"
)

var Version string

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

// getCommand. Naive way to determine what the actual user wants to do. Copy, list etc etc.
// rework when it gets more complex.
func getCommand(copyCommand bool, listCommand bool, createContainerCommand string, copyBlobCommand bool) int {

	if !copyCommand && !listCommand && createContainerCommand == "" && !copyBlobCommand {
		fmt.Println("No command given")
		os.Exit(1)
	}

	if copyCommand {
		return misc.CommandCopy
	}

	if copyBlobCommand {
		return misc.CommandCopyBlob
	}

	if listCommand {
		return misc.CommandList
	}

	if createContainerCommand != "" {
		log.Debug("createcommand issued")
		return misc.CommandCreateContainer
	}

	log.Fatal("unsure of command to use")
	return misc.CommandUnknown
}

func setupConfiguration() *misc.CloudConfig {
	config := misc.NewCloudConfig()

	var concurrentCount = flag.Uint("cc", 5, "Concurrent Count. How many blobs are copied concurrently")

	var version = flag.Bool("version", false, "Display Version")
	var source = flag.String("source", "", "Source URL")
	var dest = flag.String("dest", "", "Destination URL")
	var debug = flag.Bool("debug", false, "Debug output")
	var copyCommand = flag.Bool("copy", false, "Copy from source to destination")
	var copyBlobCommand = flag.Bool("copyblob", false, "Copy from source to destination using Azure CopyBlob flag. Can only be used if Azure is destination")

	//var copyBlobCommand = false

	var listCommand = flag.Bool("list", false, "List contents from source")
	var createContainerCommand = flag.String("createcontainer", "", "Create container for destination")

	var replace = flag.Bool("replace", true, "Replace blob if already exists")

	var azureDefaultAccountName = flag.String("AzureDefaultAccountName", "", "Default Azure Account Name")
	var azureDefaultAccountKey = flag.String("AzureDefaultAccountKey", "", "Default Azure Account Key")
	var azureSourceAccountName = flag.String("AzureSourceAccountName", "", "Source Azure Account Name")
	var azureSourceAccountKey = flag.String("AzureSourceAccountKey", "", "Source Azure Account Key")
	var azureDestAccountName = flag.String("AzureDestAccountName", "", "Destination Azure Account Name")
	var azureDestAccountKey = flag.String("AzureDestAccountKey", "", "Destination Azure Account Key")

	var s3DefaultAccessID = flag.String("S3DefaultAccessID", "", "Default S3 Access ID")
	var s3DefaultAccessSecret = flag.String("S3DefaultAccessSecret", "", "Default S3 Access Secret")
	var s3DefaultRegion = flag.String("S3DefaultRegion", "", "Default S3 Region")
	var s3SourceAccessID = flag.String("S3SourceAccessID", "", "Source S3 Access ID")
	var s3SourceAccessSecret = flag.String("S3SourceAccessSecret", "", "Source S3 Access Secret")
	var s3SourceRegion = flag.String("S3SourceRegion", "", "Source S3 Region")
	var s3DestAccessID = flag.String("S3DestAccessID", "", "Destination S3 Access ID")
	var s3DestAccessSecret = flag.String("S3DestAccessSecret", "", "Destination S3 Access Secret")
	var s3DestRegion = flag.String("S3DestRegion", "", "Destination S3 Region")

	flag.Parse()

	config.Version = *version
	config.Debug = *debug
	if !*version {

		// seems toooooo manual. Figure out something nicer later.
		if *concurrentCount > 1000 {
			fmt.Printf("Maximum number for concurrent count is 1000")
			os.Exit(1)
		}

		config.Command = getCommand(*copyCommand, *listCommand, *createContainerCommand, *copyBlobCommand)
		config.Configuration[misc.Source] = *source
		config.Configuration[misc.Dest] = *dest
		config.Replace = *replace
		config.ConcurrentCount = *concurrentCount
		config.Configuration[misc.CreateContainerName] = *createContainerCommand

		config.Configuration[misc.AzureDefaultAccountName] = *azureDefaultAccountName
		config.Configuration[misc.AzureDefaultAccountKey] = *azureDefaultAccountKey
		config.Configuration[misc.AzureSourceAccountName] = *azureSourceAccountName
		config.Configuration[misc.AzureSourceAccountKey] = *azureSourceAccountKey
		config.Configuration[misc.AzureDestAccountName] = *azureDestAccountName
		config.Configuration[misc.AzureDestAccountKey] = *azureDestAccountKey

		config.Configuration[misc.S3DefaultAccessID] = *s3DefaultAccessID
		config.Configuration[misc.S3DefaultAccessSecret] = *s3DefaultAccessSecret
		config.Configuration[misc.S3DefaultRegion] = *s3DefaultRegion
		config.Configuration[misc.S3SourceAccessID] = *s3SourceAccessID
		config.Configuration[misc.S3SourceAccessSecret] = *s3SourceAccessSecret
		config.Configuration[misc.S3SourceRegion] = *s3SourceRegion
		config.Configuration[misc.S3DestAccessID] = *s3DestAccessID
		config.Configuration[misc.S3DestAccessSecret] = *s3DestAccessSecret
		config.Configuration[misc.S3DestRegion] = *s3DestRegion
	}

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
	log.Debug("after config setup")

	// if display version, then display then exit
	if config.Version {
		fmt.Println("Version: " + Version)
		return
	}

	ac := azurecopy.NewAzureCopy(*config)

	switch config.Command {
	case misc.CommandCopy:
		err := ac.CopyBlobByURL(config.Replace, false)
		if err != nil {
			log.Fatal(err)
		}
		break

	case misc.CommandCopyBlob:
		err := ac.CopyBlobByURL(config.Replace, true)
		if err != nil {
			log.Fatal(err)
		}
		break

	case misc.CommandList:
		container, err := ac.ListContainer()
		if err != nil {
			log.Fatal(err)
		}

		log.Debug("List results")
		container.DisplayContainer("")
		break

	case misc.CommandCreateContainer:
		err := ac.CreateContainer(config.Configuration[misc.CreateContainerName])
		if err != nil {
			log.Fatal(err)
		}

	case misc.CommandUnknown:
		log.Fatal("Unsure of command to execute")
	}

}
