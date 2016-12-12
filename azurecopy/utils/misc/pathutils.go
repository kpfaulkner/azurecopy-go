package misc

// GetLastChar gets last char of string.
// SURELY there is a built in function for this??!?
func GetLastChar(str string) string {
	return str[len(str)-1:]
}
