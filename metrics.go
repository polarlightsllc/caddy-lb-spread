package lbspread

import (
	"errors"
	"strconv"

	"github.com/caddyserver/caddy/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const ns, sub = "caddy", "lb_spread"

var (
	descTracked = prometheus.NewDesc(prometheus.BuildFQName(ns, sub, "tracked_keys"),
		"Keys with routing state on this proxy.", []string{"field"}, nil)
	descSpread = prometheus.NewDesc(prometheus.BuildFQName(ns, sub, "spread_keys"),
		"Keys currently routed over more than one upstream.", []string{"field"}, nil)
	descSaturated = prometheus.NewDesc(prometheus.BuildFQName(ns, sub, "saturated_keys"),
		"Keys whose demand exceeds the replicas they may use (max_replicas or available upstreams).", []string{"field"}, nil)
	descKeyReplicas = prometheus.NewDesc(prometheus.BuildFQName(ns, sub, "key_replicas"),
		"Replicas used by the hottest spread or saturated keys (top 20 by in-flight).", []string{"field", "key"}, nil)
	descKeyDesired = prometheus.NewDesc(prometheus.BuildFQName(ns, sub, "key_desired_replicas"),
		"Uncapped replica demand of the hottest spread or saturated keys.", []string{"field", "key"}, nil)
	descKeyInflight = prometheus.NewDesc(prometheus.BuildFQName(ns, sub, "key_inflight"),
		"In-flight requests on this proxy for the hottest spread or saturated keys.", []string{"field", "key"}, nil)
	descSelections = prometheus.NewDesc(prometheus.BuildFQName(ns, sub, "selections_total"),
		"Upstream selections by rendezvous rank of the chosen upstream (0 = affinity owner).", []string{"field", "rank"}, nil)
	descSpreadSelections = prometheus.NewDesc(prometheus.BuildFQName(ns, sub, "spread_selections_total"),
		"Selections for keys that were using more than one replica.", []string{"field"}, nil)
	descScale = prometheus.NewDesc(prometheus.BuildFQName(ns, sub, "scale_events_total"),
		"Replica count changes per key.", []string{"field", "direction"}, nil)
)

type collector struct{}

func (collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		descTracked, descSpread, descSaturated, descKeyReplicas, descKeyDesired,
		descKeyInflight, descSelections, descSpreadSelections, descScale,
	} {
		ch <- d
	}
}

func (collector) Collect(ch chan<- prometheus.Metric) {
	trackers.Range(func(_, v any) bool {
		t := v.(*tracker)
		f := t.field
		snap := t.snapshot()
		ch <- prometheus.MustNewConstMetric(descTracked, prometheus.GaugeValue, float64(snap.tracked), f)
		ch <- prometheus.MustNewConstMetric(descSpread, prometheus.GaugeValue, float64(snap.spread), f)
		ch <- prometheus.MustNewConstMetric(descSaturated, prometheus.GaugeValue, float64(snap.saturated), f)
		for _, k := range snap.top {
			ch <- prometheus.MustNewConstMetric(descKeyReplicas, prometheus.GaugeValue, float64(k.replicas), f, k.key)
			ch <- prometheus.MustNewConstMetric(descKeyDesired, prometheus.GaugeValue, float64(k.desired), f, k.key)
			ch <- prometheus.MustNewConstMetric(descKeyInflight, prometheus.GaugeValue, float64(k.inflight), f, k.key)
		}
		for i := range t.selections {
			rank := strconv.Itoa(i)
			if i == rankBuckets-1 {
				rank += "+"
			}
			ch <- prometheus.MustNewConstMetric(descSelections, prometheus.CounterValue, float64(t.selections[i].Load()), f, rank)
		}
		ch <- prometheus.MustNewConstMetric(descSpreadSelections, prometheus.CounterValue, float64(t.spreadSelections.Load()), f)
		ch <- prometheus.MustNewConstMetric(descScale, prometheus.CounterValue, float64(t.scaleUps.Load()), f, "up")
		ch <- prometheus.MustNewConstMetric(descScale, prometheus.CounterValue, float64(t.scaleDowns.Load()), f, "down")
		return true
	})
}

var metricsCollector = collector{}

// registerMetrics adds the collector to the config's registry; several policy
// instances in one config share it.
func registerMetrics(ctx caddy.Context) error {
	reg := ctx.GetMetricsRegistry()
	if reg == nil {
		return nil
	}
	err := reg.Register(metricsCollector)
	var are prometheus.AlreadyRegisteredError
	if err != nil && !errors.As(err, &are) {
		return err
	}
	return nil
}
