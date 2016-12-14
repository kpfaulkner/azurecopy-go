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
	S3SourceAccessID      = "S3SourceAccessID"
	S3SourceAccessSecret  = "S3SourceAccessSecret"
	S3DestAccessID        = "S3DestAccessID"
	S3DestAccessSecret    = "S3DestAccessSecret"
)

// CloudConfig UGLY UGLY UGLY way to store the configuration.
// globally accessible, otherwise I'm passing it everywhere.
type CloudConfig struct {

	// credentials used for various handlers.
	// main application will pass these in.
	Credentials map[string]string
}

// NewCloudConfig  Make new (and only really) configuration map
func NewCloudConfig() *CloudConfig {
	cc := CloudConfig{}
	cc.Credentials = make(map[string]string)
	return &cc
}
