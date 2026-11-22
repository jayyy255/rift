package broker

import (
	"log/slog"
	"math"
	"sync/atomic"
	"time"
)

// DiskStats describes the filesystem holding the data directory.
type DiskStats struct {
	Total     uint64
	Available uint64
}

// diskMonitor tracks data-directory disk usage against the watermarks.
// Above the high watermark it logs a warning; above the critical watermark
// publishes are rejected so the disk cannot fill completely, which would make
// appends fail partway and stall fsync.
type diskMonitor struct {
	path            string
	high, critical  float64
	interval        time.Duration
	usage           func(string) (DiskStats, error)
	log             *slog.Logger
	m               *metrics
	ratio           atomic.Uint64 // float64 bits
	overCritical    atomic.Bool
	overHigh        bool
	reportedFailure bool
}

func newDiskMonitor(cfg Config, m *metrics) *diskMonitor {
	return &diskMonitor{
		path: cfg.DataDir, high: cfg.DiskHighWatermark, critical: cfg.DiskCriticalWatermark,
		interval: cfg.DiskCheckInterval, usage: cfg.DiskUsage, log: cfg.Logger, m: m,
	}
}

func (d *diskMonitor) run(stop <-chan struct{}) {
	t := time.NewTicker(d.interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			d.check()
		}
	}
}

// check samples usage and logs watermark transitions. It is only called from
// one goroutine at a time (New, then the run loop).
func (d *diskMonitor) check() {
	st, err := d.usage(d.path)
	if err != nil || st.Total == 0 {
		if !d.reportedFailure {
			d.log.Warn("disk usage unavailable; watermarks not enforced", "path", d.path, "error", err)
			d.reportedFailure = true
		}
		return
	}
	d.reportedFailure = false
	ratio := 1 - float64(st.Available)/float64(st.Total)
	d.ratio.Store(math.Float64bits(ratio))
	d.m.diskUsedRatio.Set(ratio)

	critical := ratio >= d.critical
	if critical != d.overCritical.Load() {
		d.overCritical.Store(critical)
		if critical {
			d.m.diskCritical.Set(1)
			d.log.Error("disk usage above critical watermark; rejecting publishes",
				"used_ratio", ratio, "critical_watermark", d.critical)
		} else {
			d.m.diskCritical.Set(0)
			d.log.Info("disk usage below critical watermark; accepting publishes", "used_ratio", ratio)
		}
	}
	high := ratio >= d.high
	if high && !d.overHigh {
		d.log.Warn("disk usage above high watermark", "used_ratio", ratio, "high_watermark", d.high)
	} else if !high && d.overHigh {
		d.log.Info("disk usage back below high watermark", "used_ratio", ratio)
	}
	d.overHigh = high
}

func (d *diskMonitor) isCritical() bool { return d.overCritical.Load() }
