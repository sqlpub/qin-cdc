package utils

import (
	"flag"
	"github.com/go-demo/version"
	"github.com/siddontang/go-log/log"
	"os"
)

type Help struct {
	printVersion bool
	ConfigFile   *string
	LogLevel     *string
	LogFile      *string
	Daemon       *bool
	HttpPort     *uint
}

func InitHelp() (help *Help) {
	help = &Help{}
	help.ConfigFile = flag.String("config", "", "config file")
	help.LogLevel = flag.String("level", "info", "log level")
	help.LogFile = flag.String("log-file", "qin-cdc.log", "log file")
	help.Daemon = flag.Bool("daemon", false, "daemon run, must specify param 'log-file'")
	help.HttpPort = flag.Uint("http-port", 7716, "http monitor port, curl http://localhost:7716/metrics")
	flag.BoolVar(&help.printVersion, "version", false, "print program build version")
	flag.Parse()
	if help.printVersion {
		version.PrintVersion()
		os.Exit(0)
	}
	log.Infof("starting version: %s", version.Version)
	return help
}
