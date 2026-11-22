package broker

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jayyy255/rift/internal/logstore"
	"github.com/jayyy255/rift/internal/version"
)

type metrics struct {
	reg *prometheus.Registry

	produceRecords  *prometheus.CounterVec
	produceBytes    *prometheus.CounterVec
	produceRequests *prometheus.CounterVec
	produceLatency  *prometheus.HistogramVec
	fsyncLatency    prometheus.Histogram
	fetchRecords    *prometheus.CounterVec
	fetchRequests   prometheus.Counter
	commits         prometheus.Counter
	rejected        *prometheus.CounterVec
	corruptReads    prometheus.Counter

	recoveryTruncations  prometheus.Counter
	recoveryTruncated    prometheus.Counter
	recoveryRebuiltIndex prometheus.Counter
	offsetsClamped       prometheus.Counter
	retentionSegments    *prometheus.CounterVec
	retentionBytes       *prometheus.CounterVec
	diskUsedRatio        prometheus.Gauge
	diskCritical         prometheus.Gauge
}

func newMetrics(b *Broker) *metrics {
	m := &metrics{reg: prometheus.NewRegistry()}
	f := func(c prometheus.Collector) { m.reg.MustRegister(c) }
	latency := []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}

	m.produceRecords = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rift_produce_records_total", Help: "Records appended."}, []string{"topic"})
	m.produceBytes = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rift_produce_bytes_total", Help: "Encoded record bytes appended."}, []string{"topic"})
	m.produceRequests = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rift_produce_requests_total", Help: "Publish requests by durability and result."}, []string{"durability", "result"})
	m.produceLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "rift_produce_latency_seconds", Help: "Publish request latency until acknowledgment.", Buckets: latency}, []string{"durability"})
	m.fsyncLatency = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "rift_fsync_latency_seconds", Help: "Latency of fsyncs performed for FSYNCED publishes.", Buckets: latency})
	m.fetchRecords = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rift_fetch_records_total", Help: "Records returned to consumers."}, []string{"topic"})
	m.fetchRequests = prometheus.NewCounter(prometheus.CounterOpts{Name: "rift_fetch_requests_total", Help: "Fetch requests served."})
	m.commits = prometheus.NewCounter(prometheus.CounterOpts{Name: "rift_offset_commits_total", Help: "Consumer offset commit requests."})
	m.rejected = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rift_produce_rejected_total", Help: "Publish requests rejected before append, by reason."}, []string{"reason"})
	m.corruptReads = prometheus.NewCounter(prometheus.CounterOpts{Name: "rift_storage_corrupt_reads_total", Help: "Reads that hit records failing checksum or format validation."})
	m.recoveryTruncations = prometheus.NewCounter(prometheus.CounterOpts{Name: "rift_storage_recovery_truncations_total", Help: "Partitions whose active segment had an invalid tail truncated at startup."})
	m.recoveryTruncated = prometheus.NewCounter(prometheus.CounterOpts{Name: "rift_storage_recovery_truncated_bytes_total", Help: "Bytes removed by startup recovery."})
	m.recoveryRebuiltIndex = prometheus.NewCounter(prometheus.CounterOpts{Name: "rift_storage_recovery_rebuilt_indexes_total", Help: "Segment indexes rebuilt at startup."})
	m.offsetsClamped = prometheus.NewCounter(prometheus.CounterOpts{Name: "rift_consumer_offsets_clamped_total", Help: "Committed offsets lowered to a partition's recovered end offset."})
	m.retentionSegments = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rift_retention_deleted_segments_total", Help: "Segments deleted by retention."}, []string{"topic"})
	m.retentionBytes = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rift_retention_deleted_bytes_total", Help: "Bytes deleted by retention."}, []string{"topic"})
	m.diskUsedRatio = prometheus.NewGauge(prometheus.GaugeOpts{Name: "rift_disk_used_ratio", Help: "Used/total space of the data directory's filesystem."})
	m.diskCritical = prometheus.NewGauge(prometheus.GaugeOpts{Name: "rift_disk_critical", Help: "1 while disk usage is above the critical watermark and publishes are rejected."})

	for _, c := range []prometheus.Collector{
		m.produceRecords, m.produceBytes, m.produceRequests, m.produceLatency, m.fsyncLatency,
		m.fetchRecords, m.fetchRequests, m.commits, m.rejected, m.corruptReads,
		m.recoveryTruncations, m.recoveryTruncated, m.recoveryRebuiltIndex, m.offsetsClamped,
		m.retentionSegments, m.retentionBytes, m.diskUsedRatio, m.diskCritical,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "rift_build_info",
			Help:        "Build information for the running broker; always 1.",
			ConstLabels: prometheus.Labels{"version": version.Version, "node_id": b.cfg.NodeID},
		}, func() float64 { return 1 }),
		&stateCollector{b: b},
	} {
		f(c)
	}
	return m
}

func (m *metrics) handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

func (m *metrics) observeRecovery(r logstore.RecoveryReport) {
	if r.Truncation != nil {
		m.recoveryTruncations.Inc()
		m.recoveryTruncated.Add(float64(r.Truncation.Bytes))
	}
	m.recoveryRebuiltIndex.Add(float64(r.RebuiltIndexes))
}

func (m *metrics) observeRetention(topic string, _ int32, res logstore.RetentionResult) {
	m.retentionSegments.WithLabelValues(topic).Add(float64(res.Segments))
	m.retentionBytes.WithLabelValues(topic).Add(float64(res.Bytes))
}

// stateCollector reports partition offsets, sizes, and consumer lag at
// scrape time, so gauges never go stale after topic deletion.
type stateCollector struct{ b *Broker }

var (
	descStart    = prometheus.NewDesc("rift_partition_log_start_offset", "Oldest retained offset.", []string{"topic", "partition"}, nil)
	descEnd      = prometheus.NewDesc("rift_partition_high_watermark", "Offset the next record will receive.", []string{"topic", "partition"}, nil)
	descSynced   = prometheus.NewDesc("rift_partition_synced_offset", "Records below this offset are fsynced.", []string{"topic", "partition"}, nil)
	descSize     = prometheus.NewDesc("rift_partition_size_bytes", "Bytes stored for the partition.", []string{"topic", "partition"}, nil)
	descSegments = prometheus.NewDesc("rift_partition_segments", "Segment files in the partition.", []string{"topic", "partition"}, nil)
	descLag      = prometheus.NewDesc("rift_consumer_group_lag", "High watermark minus committed offset.", []string{"group", "topic", "partition"}, nil)
)

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descStart, descEnd, descSynced, descSize, descSegments, descLag} {
		ch <- d
	}
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	if c.b.topics == nil {
		return
	}
	ends := map[string]map[int32]int64{}
	for _, t := range c.b.topics.List() {
		name := t.Metadata().Name
		ends[name] = map[int32]int64{}
		for p := range t.Partitions() {
			l, err := t.Log(p)
			if err != nil {
				continue
			}
			st := l.Stats()
			ps := strconv.Itoa(int(p))
			ch <- prometheus.MustNewConstMetric(descStart, prometheus.GaugeValue, float64(st.StartOffset), name, ps)
			ch <- prometheus.MustNewConstMetric(descEnd, prometheus.GaugeValue, float64(st.EndOffset), name, ps)
			ch <- prometheus.MustNewConstMetric(descSynced, prometheus.GaugeValue, float64(st.SyncedOffset), name, ps)
			ch <- prometheus.MustNewConstMetric(descSize, prometheus.GaugeValue, float64(st.SizeBytes), name, ps)
			ch <- prometheus.MustNewConstMetric(descSegments, prometheus.GaugeValue, float64(st.Segments), name, ps)
			ends[name][p] = st.EndOffset
		}
	}
	if c.b.offsets == nil {
		return
	}
	for _, e := range c.b.offsets.Snapshot() {
		end, ok := ends[e.Topic][e.Partition]
		if !ok {
			continue
		}
		ch <- prometheus.MustNewConstMetric(descLag, prometheus.GaugeValue, float64(max(end-e.Offset, 0)),
			e.Group, e.Topic, strconv.Itoa(int(e.Partition)))
	}
}
