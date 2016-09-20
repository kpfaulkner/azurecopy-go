package models

type Blob struct {

	// data.
	data   []byte
	name   string
	url    string
	origin CloudOrigin
}
