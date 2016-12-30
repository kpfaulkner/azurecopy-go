package misc

import (
	"crypto/md5"
	"encoding/hex"
)

// GetLastChar gets last char of string.
// SURELY there is a built in function for this??!?
func GetLastChar(str string) string {
	return str[len(str)-1:]
}

func GenerateCacheName(path string) string {
	hasher := md5.New()
	hasher.Write([]byte(path))
	return hex.EncodeToString(hasher.Sum(nil))
}
