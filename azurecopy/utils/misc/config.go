package misc

// misc consts for credentials.
// need a more dynamic way to add for new cloud types.
// but for now, it will do.
const (
	// Azure.
	AzureDefaultAccountName = "AzureDefaultAccountName"
	AzureDefaultAccountKey  = "AzureDefaultAccountKey"
	AzureSourceAccountName  = "AzureSourceAccountName"
	AzureSourceAccountKey   = "AzureSourceAccountKey"
	AzureDestAccountName    = "AzureDestAccountName"
	AzureDestAccountKey     = "AzureDestAccountKey"

	// S3
	S3DefaultAccessID     = "S3DefaultAccessID"
	S3DefaultAccessSecret = "S3DefaultAccessSecret"
	S3DefaultRegion       = "S3DefaultRegion"

	S3SourceAccessID     = "S3SourceAccessID"
	S3SourceAccessSecret = "S3SourceAccessSecret"
	S3SourceRegion       = "S3SourceRegion"

	S3DestAccessID     = "S3DestAccessID"
	S3DestAccessSecret = "S3DestAccessSecret"
	S3DestRegion       = "S3DestRegion"

	// debug
	Debug   = "Debug"
	Source  = "Source"
	Dest    = "Dest"
	Replace = "Replace"

	// container name to create.
	CreateContainerName = "CreateContainer"
)

// CloudConfig UGLY UGLY UGLY way to store the configuration.
// globally accessible, otherwise I'm passing it everywhere.
type CloudConfig struct {
	Configuration map[string]string

	Debug bool // are we in debug mode.

	Command int // command we're executing

	Replace bool // will replace at destination

	Version bool // display version
}

// NewCloudConfig  Make new (and only really) configuration map
func NewCloudConfig() *CloudConfig {
	cc := CloudConfig{}
	cc.Configuration = make(map[string]string)
	return &cc
}
