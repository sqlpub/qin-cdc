package utils

import (
	"github.com/siddontang/go-log/log"
	"os"
	"path/filepath"
)

func GetExecPath() string {
	ex, err := os.Executable()
	if err != nil {
		log.Fatal("get exec path error: ", err)
	}
	exPath := filepath.Dir(ex)
	return exPath
}
