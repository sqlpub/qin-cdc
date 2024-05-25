package main

import (
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/app"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/utils"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// help handle
	inputParam := utils.InitHelp()
	// input param handle
	utils.InputParamHandle(inputParam)
	// init log level
	log.SetLevelByName(*inputParam.LogLevel)
	// daemon mode handle
	utils.Daemon(inputParam)

	// 进程信号处理
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	utils.StartHttp(inputParam)

	// config file handle
	conf := config.NewConfig(inputParam.ConfigFile)
	s, err := app.NewServer(conf)
	if err != nil {
		log.Fatal(err)
	}
	s.Start()

	utils.InitHttpApi()

	select {
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
		s.Close()
		log.Infof("qin-cdc is stopped.")
	}
}
