package s3gof3r

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

// Keys for an Amazon Web Services account.
// Used for signing http requests.
type Keys struct {
	AccessKey     string
	SecretKey     string
	SecurityToken string
}

type mdCreds struct {
	Code            string
	LastUpdated     string
	Type            string
	AccessKeyID     string `xml:"AccessKeyId"`
	SecretAccessKey string
	Token           string
	Expiration      string
}

// InstanceKeys Requests the AWS keys from the instance-based metadata on EC2
// Assumes only one IAM role.
func InstanceKeys() (Keys, error) {
	rolePath := "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
	var creds mdCreds

	// request the role name for the instance
	// assumes there is only one
	resp, err := ClientWithTimeout(2 * time.Second).Get(rolePath)
	if err != nil {
		return Keys{}, err
	}
	if resp.StatusCode != 200 {
		return Keys{}, newRespError(resp)
	}

	role, err := ioutil.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if err != nil {
		return Keys{}, err
	}
	if closeErr != nil {
		return Keys{}, closeErr
	}

	// request the credential metadata for the role
	resp, err = http.Get(rolePath + string(role))
	if err != nil {
		return Keys{}, err
	}
	if resp.StatusCode != 200 {
		return Keys{}, newRespError(resp)
	}

	metadata, err := ioutil.ReadAll(resp.Body)
	closeErr = resp.Body.Close()
	if err != nil {
		return Keys{}, err
	}
	if closeErr != nil {
		return Keys{}, closeErr
	}

	if err = json.Unmarshal([]byte(metadata), &creds); err != nil {
		return Keys{}, err
	}

	return Keys{
		AccessKey:     creds.AccessKeyID,
		SecretKey:     creds.SecretAccessKey,
		SecurityToken: creds.Token,
	}, nil
}

// EnvKeys Reads the AWS keys from the environment
func EnvKeys() (keys Keys, err error) {
	keys = Keys{
		AccessKey:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey:     os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SecurityToken: os.Getenv("AWS_SECURITY_TOKEN"),
	}
	if keys.AccessKey == "" || keys.SecretKey == "" {
		err = fmt.Errorf("keys not set in environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
	}
	return
}
