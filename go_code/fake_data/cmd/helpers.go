package cmd

import (
	"os/user"
	"path/filepath"
	"strings"
)

func resolveOutputDir(outputDir string) (string, error) {

	var finalOutputDir string = ""

	if outputDir == "~" || strings.HasPrefix(outputDir, "~/") {
		usr, err := user.Current()
		if err != nil {
			return "", err
		}

		if outputDir == "~" {
			finalOutputDir = usr.HomeDir
		} else {
			finalOutputDir = filepath.Join(usr.HomeDir, outputDir[2:])
		}
	} else {
		finalOutputDir = outputDir
	}

	//add a trailing slash if needed
	if !strings.HasSuffix(finalOutputDir, "/") {
		finalOutputDir += "/"
	}

	//fmt.Printf("final output dir is %s: ", finalOutputDir)
	return finalOutputDir, nil

}
