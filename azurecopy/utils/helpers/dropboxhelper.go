package helpers

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	log "github.com/Sirupsen/logrus"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	homedir "github.com/mitchellh/go-homedir"
	"golang.org/x/oauth2"
)

const (
	configFileName = "azurecopyauth.json"
	appKey         = "XXX"
	appSecret      = "XXX"
	dropboxScheme  = "dropbox"

	tokenPersonal   = "personal"
	tokenTeamAccess = "teamAccess"
	tokenTeamManage = "teamManage"
)

// Map of map of strings
// For each domain, we want to save different tokens depending on the
// command type: personal, team access and team manage
type TokenMap map[string]map[string]string

func WriteTokens(filePath string, tokens TokenMap) {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Doesn't exist; lets create it
		err = os.MkdirAll(filepath.Dir(filePath), 0700)
		if err != nil {
			return
		}
	}

	// At this point, file must exist. Lets (over)write it.
	b, err := json.Marshal(tokens)
	if err != nil {
		return
	}
	if err = ioutil.WriteFile(filePath, b, 0600); err != nil {
		return
	}
}

func ReadTokens(filePath string) (TokenMap, error) {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var tokens TokenMap
	if json.Unmarshal(b, &tokens) != nil {
		return nil, err
	}

	return tokens, nil
}

func SetupConnection() (*dropbox.Config, error) {
	conf := oauth2.Config{
		ClientID:     appKey,
		ClientSecret: appSecret,
		Endpoint:     dropbox.OAuthEndpoint(""),
	}

	dir, err := homedir.Dir()
	if err != nil {
		return nil, err
	}
	filePath := path.Join(dir, ".config", "azurecopy", configFileName)
	tokType := tokenPersonal

	tokenMap, err := ReadTokens(filePath)
	if tokenMap == nil {
		tokenMap = make(TokenMap)
	}
	domain := ""

	if tokenMap[domain] == nil {
		tokenMap[domain] = make(map[string]string)
	}
	tokens := tokenMap[domain]

	if err != nil || tokens[tokType] == "" {
		fmt.Printf("1. Go to %v\n", conf.AuthCodeURL("state"))
		fmt.Printf("2. Click \"Allow\" (you might have to log in first).\n")
		fmt.Printf("3. Copy the authorization code.\n")
		fmt.Printf("Enter the authorization code here: ")

		var code string
		if _, err = fmt.Scan(&code); err != nil {
			return nil, err
		}
		var token *oauth2.Token
		token, err = conf.Exchange(oauth2.NoContext, code)
		if err != nil {
			return nil, err
		}
		tokens[tokType] = token.AccessToken
		WriteTokens(filePath, tokenMap)
	} else {
		log.Debugf("Already have Dropbox token")
	}

	config := dropbox.Config{tokens[tokType], true, "", domain}

	return &config, nil
}
