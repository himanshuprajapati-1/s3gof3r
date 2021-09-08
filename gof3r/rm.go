package main

import (
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/github/s3gof3r"
	"github.com/jessevdk/go-flags"
)

type RmOpts struct {
	CommonOpts
	VersionID string `short:"v" long:"versionId" description:"version ID of the object version to delete" no-ini:"true"`
}

func (rm *RmOpts) Usage() string {
	return "<path> [rm-OPTIONS]"
}

func (rm *RmOpts) Execute(args []string) error {

	k, err := getAWSKeys()
	if err != nil {
		return err
	}

	conf := new(s3gof3r.Config)
	*conf = *s3gof3r.DefaultConfig
	s3 := s3gof3r.New(rm.EndPoint, k)
	s3gof3r.SetLogger(os.Stderr, "", log.Ltime, rm.Debug)

	// parse positional cp args
	if len(args) != 1 {
		return fmt.Errorf("rm: path argument required")
	}

	//var urls [1]*url.URL
	u, err := url.ParseRequestURI(args[0])
	if err != nil {
		return fmt.Errorf("parse error: %s", err)
	}
	if u.Host != "" && u.Scheme != "s3" {
		return fmt.Errorf("parse error: %s", u.String())
	}
	return s3.Bucket(u.Host).Delete(u.Path)
}

func addRmOpts(opts *RmOpts, parser *flags.Parser) {
	cmd, err := parser.AddCommand("rm", "delete from S3", "", opts)
	if err != nil {
		log.Fatal(err)
	}
	cmd.ArgsRequired = true
}
