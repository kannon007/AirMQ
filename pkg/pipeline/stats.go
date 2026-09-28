package pipeline

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)

// ProcessorStats represents an immutable snapshot of a processor's runtime performance metrics.
type ProcessorStats struct {
	Name         string        `json:"name"`
	Invocations  int64         `json:"invocations"`
	MatchedCount int64         `json:"matched_count"`
	SkippedCount int64         `json:"skipped_count"`
	SuccessCount int64         `json:"success_count"`
	DroppedCount int64         `json:"dropped_count"`
	ErrorCount   int64         `json:"error_count"`
	LastDuration time.Duration `json:"last_duration"`
	AvgDuration  time.Duration `json:"avg_duration"`
	MaxDuration  time.Duration `json:"max_duration"`
}

// processorMetricCollector tracks high-throughput, lock-free metrics for a single processor.
// Uses cache-friendly single atomic operations where possible.
type processorMetricCollector struct {
	name            string
	matchedCount    atomic.Int64
	skippedCount    atomic.Int64
	droppedCount    atomic.Int64
	errorCount      atomic.Int64
	totalDurationNs atomic.Int64
	lastDurationNs  atomic.Int64
	maxDurationNs   atomic.Int64
}

func newProcessorMetricCollector(name string) *processorMetricCollector {
	return &processorMetricCollector{name: name}
}

func (m *processorMetricCollector) recordSkipped() {
	m.skippedCount.Add(1)
}

func (m *processorMetricCollector) recordQuickExecution(dropped bool, err error) {
	m.matchedCount.Add(1)
	if err != nil {
		m.errorCount.Add(1)
	} else if dropped {
		m.droppedCount.Add(1)
	}
}

func (m *processorMetricCollector) recordExecution(elapsed time.Duration, dropped bool, err error) {
	m.matchedCount.Add(1)

	ns := elapsed.Nanoseconds()
	m.totalDurationNs.Add(ns)
	m.lastDurationNs.Store(ns)

	// Avoid CAS loop if ns is lower than current maximum
	if curMax := m.maxDurationNs.Load(); ns > curMax {
		for {
			curMax = m.maxDurationNs.Load()
			if ns <= curMax {
				break
			}
			if m.maxDurationNs.CompareAndSwap(curMax, ns) {
				break
			}
		}
	}

	if err != nil {
		m.errorCount.Add(1)
	} else if dropped {
		m.droppedCount.Add(1)
	}
}

// Snapshot returns an immutable point-in-time snapshot of the metrics.
func (m *processorMetricCollector) Snapshot() ProcessorStats {
	matched := m.matchedCount.Load()
	skipped := m.skippedCount.Load()
	invocations := matched + skipped
	dropped := m.droppedCount.Load()
	errs := m.errorCount.Load()

	success := matched - dropped - errs
	if success < 0 {
		success = 0
	}

	totalNs := m.totalDurationNs.Load()
	lastNs := m.lastDurationNs.Load()
	maxNs := m.maxDurationNs.Load()

	var avgDuration time.Duration
	if matched > 0 {
		avgDuration = time.Duration(totalNs / matched)
	}

	return ProcessorStats{
		Name:         m.name,
		Invocations:  invocations,
		MatchedCount: matched,
		SkippedCount: skipped,
		SuccessCount: success,
		DroppedCount: dropped,
		ErrorCount:   errs,
		LastDuration: time.Duration(lastNs),
		AvgDuration:  avgDuration,
		MaxDuration:  time.Duration(maxNs),
	}
}

// FormatStatsTable formats an array of ProcessorStats into an ASCII table string.
func FormatStatsTable(stats []ProcessorStats) string {
	var buf bytes.Buffer
	buf.WriteString("\n================================= 管道各处理器实时性能与健康度 =================================\n")
	buf.WriteString(fmt.Sprintf("%-18s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n",
		"处理器名称", "调度总数", "匹配执行", "预判跳过", "拦截丢弃", "平均耗时", "最大耗时"))
	buf.WriteString("-----------------------------------------------------------------------------------------------\n")

	for _, s := range stats {
		buf.WriteString(fmt.Sprintf("%-18s | %-10d | %-10d | %-10d | %-10d | %-10v | %-10v\n",
			s.Name, s.Invocations, s.MatchedCount, s.SkippedCount, s.DroppedCount, s.AvgDuration, s.MaxDuration))
	}
	buf.WriteString("===============================================================================================\n")
	return buf.String()
}
