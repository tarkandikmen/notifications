package metrics

// PublishLagSample wires a kafkaadmin.MaxLag result onto the
// kafka_consumer_lag gauge. Called from dispatcher.runOnce and
// reaper.runOnce after MaxLag returns. On err != nil the gauge is
// left untouched so a brief admin-call blip doesn't reset the gauge
// to zero — the next successful sample ticks the gauge into its new
// value.
//
// The helper exists so dispatcher and reaper share one publishing
// site; a future change to the gauge's semantic (e.g., "set to NaN
// on error" vs. "leave untouched") lands in one place.
//
// docs/phases/05-observability.md §1.4.
func PublishLagSample(group, topic string, lag int64, err error) {
	if err != nil {
		return
	}
	KafkaConsumerLag.WithLabelValues(group, topic).Set(float64(lag))
}
