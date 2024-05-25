package utils

import (
	"github.com/siddontang/go-log/log"
	"os"
	"path/filepath"
)

func InputParamHandle(inputParam *Help) {
	if *inputParam.ConfigFile == "" {
		log.Infof("-config param does not exist!")
		os.Exit(0)
	} else {
		abs, err := filepath.Abs(*inputParam.ConfigFile)
		if err != nil {
			log.Fatal("-config abs error: ", err.Error())
		}
		*inputParam.ConfigFile = abs
	}
	if *inputParam.Daemon {
		if *inputParam.LogFile == "" {
			log.Infof("daemon mode, must specify -log-file param!")
			os.Exit(0)
		}
	}
}
