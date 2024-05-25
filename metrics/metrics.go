package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var OpsStartTime = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "qin_cdc",
	Subsystem: "start",
	Name:      "time",
	Help:      "qin-cdc startup timestamp（s）.",
})

var OpsReadProcessed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "qin_cdc_read_processed_ops_total",
	Help: "The total number of read processed events",
})

var OpsWriteProcessed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "qin_cdc_write_processed_ops_total",
	Help: "The total number of write processed events",
})

var DelayReadTime = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "qin_cdc",
	Subsystem: "read_delay",
	Name:      "time_seconds",
	Help:      "Delay in seconds to read the binlog at the source.",
})

var DelayWriteTime = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "qin_cdc",
	Subsystem: "write_delay",
	Name:      "time_seconds",
	Help:      "Delay in seconds to write at the destination.",
})

func init() {
	prometheus.MustRegister(OpsStartTime, DelayReadTime, DelayWriteTime)
}
