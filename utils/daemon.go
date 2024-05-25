package utils

import (
	"github.com/sevlyar/go-daemon"
	"github.com/siddontang/go-log/log"
)

func Daemon(inputParam *Help) {
	if *inputParam.Daemon {
		cntxt := &daemon.Context{
			PidFileName: GetExecPath() + "/qin-cdc.pid",
			PidFilePerm: 0644,
			LogFileName: *inputParam.LogFile,
			LogFilePerm: 0640,
			WorkDir:     "./",
			Umask:       027,
		}
		d, err := cntxt.Reborn()
		if err != nil {
			log.Fatal("daemon mode run failed, err: ", err)
		}

		if d != nil {
			return
		}
		defer func(cntxt *daemon.Context) {
			err = cntxt.Release()
			if err != nil {
				log.Fatal("daemon release failed, err: ", err)
			}
		}(cntxt)
	}
}
