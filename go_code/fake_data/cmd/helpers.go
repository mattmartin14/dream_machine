package cmd

import (
	"os/user"
	"path/filepath"
	"strconv"
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

func format_nbr_with_commas(rows int) string {
	str := strconv.Itoa(rows)
	var result string
	for i, v := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result += ","
		}
		result += string(v)
	}
	return result
}
