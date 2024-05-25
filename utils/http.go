package utils

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/api"
	"github.com/sqlpub/qin-cdc/metrics"
	"net/http"
	"time"
)

func StartHttp(inputParam *Help) {
	// Start prometheus http monitor
	go func() {
		metrics.OpsStartTime.Set(float64(time.Now().Unix()))
		log.Infof("starting http on port %d", *inputParam.HttpPort)
		http.Handle("/metrics", promhttp.Handler())
		httpPortAddr := fmt.Sprintf(":%d", *inputParam.HttpPort)
		err := http.ListenAndServe(httpPortAddr, nil)
		if err != nil {
			log.Fatalf("starting http monitor error: %v", err)
		}
	}()
}

func InitHttpApi() {
	http.HandleFunc("/api/addRouter", api.AddRouter())
	http.HandleFunc("/api/delRule", api.DelRouter())
	http.HandleFunc("/api/getRule", api.GetRouter())
	http.HandleFunc("/api/pause", api.PauseRouter())
	http.HandleFunc("/api/resume", api.ResumeRouter())
}
