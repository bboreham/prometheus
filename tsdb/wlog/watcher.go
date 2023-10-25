// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wlog

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb/record"
)

const (
	checkpointPeriod   = 5 * time.Second
	segmentCheckPeriod = 100 * time.Millisecond
	consumer           = "consumer"
)

var (
	ErrIgnorable = errors.New("ignore me")
	readTimeout  = 15 * time.Second
)

// WriteTo is an interface used by the Watcher to send the samples it's read
// from the WAL on to somewhere else. Functions will be called concurrently
// and it is left to the implementer to make sure they are safe.
type WriteTo interface {
	// Append and AppendExemplar should block until the samples are fully accepted,
	// whether enqueued in memory or successfully written to it's final destination.
	// Once returned, the WAL Watcher will not attempt to pass that data again.
	Append([]record.RefSample) bool
	AppendExemplars([]record.RefExemplar) bool
	AppendHistograms([]record.RefHistogramSample) bool
	AppendFloatHistograms([]record.RefFloatHistogramSample) bool
	StoreSeries([]record.RefSeries, int)

	// Next two methods are intended for garbage-collection: first we call
	// UpdateSeriesSegment on all current series
	UpdateSeriesSegment([]record.RefSeries, int)
	// Then SeriesReset is called to allow the deletion
	// of all series created in a segment lower than the argument.
	SeriesReset(int)
}

// Used to notifier the watcher that data has been written so that it can read.
type WriteNotified interface {
	Notify()
}

type WatcherMetrics struct {
	recordsRead           *prometheus.CounterVec
	recordDecodeFails     *prometheus.CounterVec
	samplesSentPreTailing *prometheus.CounterVec
	currentSegment        *prometheus.GaugeVec
	notificationsSkipped  *prometheus.CounterVec
}

// Watcher watches the TSDB WAL for a given WriteTo.
type Watcher struct {
	name           string
	writer         WriteTo
	logger         log.Logger
	walDir         string
	lastCheckpoint string
	sendExemplars  bool
	sendHistograms bool
	metrics        *WatcherMetrics
	readerMetrics  *LiveReaderMetrics

	startTime      time.Time
	startTimestamp int64 // the start time as a Prometheus timestamp
	sendSamples    bool

	recordsReadMetric       *prometheus.CounterVec
	recordDecodeFailsMetric prometheus.Counter
	samplesSentPreTailing   prometheus.Counter
	currentSegmentMetric    prometheus.Gauge
	notificationsSkipped    prometheus.Counter

	readNotify chan struct{}
	quit       chan struct{}
	done       chan struct{}

	// For testing, stop when we hit this segment.
	MaxSegment int
}

func NewWatcherMetrics(reg prometheus.Registerer) *WatcherMetrics {
	m := &WatcherMetrics{
		recordsRead: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "prometheus",
				Subsystem: "wal_watcher",
				Name:      "records_read_total",
				Help:      "Number of records read by the WAL watcher from the WAL.",
			},
			[]string{consumer, "type"},
		),
		recordDecodeFails: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "prometheus",
				Subsystem: "wal_watcher",
				Name:      "record_decode_failures_total",
				Help:      "Number of records read by the WAL watcher that resulted in an error when decoding.",
			},
			[]string{consumer},
		),
		samplesSentPreTailing: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "prometheus",
				Subsystem: "wal_watcher",
				Name:      "samples_sent_pre_tailing_total",
				Help:      "Number of sample records read by the WAL watcher and sent to remote write during replay of existing WAL.",
			},
			[]string{consumer},
		),
		currentSegment: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "prometheus",
				Subsystem: "wal_watcher",
				Name:      "current_segment",
				Help:      "Current segment the WAL watcher is reading records from.",
			},
			[]string{consumer},
		),
		notificationsSkipped: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "prometheus",
				Subsystem: "wal_watcher",
				Name:      "notifications_skipped_total",
				Help:      "The number of WAL write notifications that the Watcher has skipped due to already being in a WAL read routine.",
			},
			[]string{consumer},
		),
	}

	if reg != nil {
		reg.MustRegister(m.recordsRead)
		reg.MustRegister(m.recordDecodeFails)
		reg.MustRegister(m.samplesSentPreTailing)
		reg.MustRegister(m.currentSegment)
		reg.MustRegister(m.notificationsSkipped)
	}

	return m
}

// NewWatcher creates a new WAL watcher for a given WriteTo.
func NewWatcher(metrics *WatcherMetrics, readerMetrics *LiveReaderMetrics, logger log.Logger, name string, writer WriteTo, dir string, sendExemplars, sendHistograms bool) *Watcher {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Watcher{
		logger:         logger,
		writer:         writer,
		metrics:        metrics,
		readerMetrics:  readerMetrics,
		walDir:         filepath.Join(dir, "wal"),
		name:           name,
		sendExemplars:  sendExemplars,
		sendHistograms: sendHistograms,

		readNotify: make(chan struct{}),
		quit:       make(chan struct{}),
		done:       make(chan struct{}),

		MaxSegment: -1,
	}
}

func (w *Watcher) Notify() {
	select {
	case w.readNotify <- struct{}{}:
		return
	default: // default so we can exit
		// we don't need a buffered channel or any buffering since
		// for each notification it recv's the watcher will read until EOF
		w.notificationsSkipped.Inc()
	}
}

func (w *Watcher) setMetrics() {
	// Setup the WAL Watchers metrics. We do this here rather than in the
	// constructor because of the ordering of creating Queue Managers's,
	// stopping them, and then starting new ones in storage/remote/storage.go ApplyConfig.
	if w.metrics != nil {
		w.recordsReadMetric = w.metrics.recordsRead.MustCurryWith(prometheus.Labels{consumer: w.name})
		w.recordDecodeFailsMetric = w.metrics.recordDecodeFails.WithLabelValues(w.name)
		w.samplesSentPreTailing = w.metrics.samplesSentPreTailing.WithLabelValues(w.name)
		w.currentSegmentMetric = w.metrics.currentSegment.WithLabelValues(w.name)
		w.notificationsSkipped = w.metrics.notificationsSkipped.WithLabelValues(w.name)

	}
}

// Start the Watcher.
func (w *Watcher) Start() {
	w.setMetrics()
	level.Info(w.logger).Log("msg", "Starting WAL watcher", "queue", w.name)

	go w.loop()
}

// Stop the Watcher.
func (w *Watcher) Stop() {
	close(w.quit)
	<-w.done

	// Records read metric has series and samples.
	if w.metrics != nil {
		w.metrics.recordsRead.DeleteLabelValues(w.name, "series")
		w.metrics.recordsRead.DeleteLabelValues(w.name, "samples")
		w.metrics.recordDecodeFails.DeleteLabelValues(w.name)
		w.metrics.samplesSentPreTailing.DeleteLabelValues(w.name)
		w.metrics.currentSegment.DeleteLabelValues(w.name)
	}

	level.Info(w.logger).Log("msg", "WAL watcher stopped", "queue", w.name)
}

func (w *Watcher) loop() {
	defer close(w.done)

	// We may encounter failures processing the WAL; we should wait and retry.
	for !isClosed(w.quit) {
		w.SetStartTime(time.Now())
		if err := w.Run(); err != nil {
			level.Error(w.logger).Log("msg", "error tailing WAL", "err", err)
		}

		select {
		case <-w.quit:
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// Run the watcher, which will tail the WAL until the quit channel is closed
// or an error case is hit.
func (w *Watcher) Run() error {
	_, lastSegment, err := w.firstAndLast()
	if err != nil {
		return errors.Wrap(err, "wal.Segments")
	}

	// We want to ensure this is false across iterations since
	// Run will be called again if there was a failure to read the WAL.
	w.sendSamples = false

	level.Info(w.logger).Log("msg", "Replaying WAL", "queue", w.name)

	// Backfill from the checkpoint first if it exists.
	lastCheckpoint, checkpointIndex, err := LastCheckpoint(w.walDir)
	if err != nil && err != record.ErrNotFound {
		return errors.Wrap(err, "tsdb.LastCheckpoint")
	}

	if err == nil {
		if err = w.readCheckpoint(lastCheckpoint, newPassToWriter()); err != nil {
			return errors.Wrap(err, "readCheckpoint")
		}
	}
	w.lastCheckpoint = lastCheckpoint

	currentSegment, err := w.findSegmentForIndex(checkpointIndex)
	if err != nil {
		return err
	}

	level.Debug(w.logger).Log("msg", "Tailing WAL", "lastCheckpoint", lastCheckpoint, "checkpointIndex", checkpointIndex, "currentSegment", currentSegment, "lastSegment", lastSegment)
	for !isClosed(w.quit) {
		w.currentSegmentMetric.Set(float64(currentSegment))
		level.Debug(w.logger).Log("msg", "Processing segment", "currentSegment", currentSegment)

		// On start, after reading the existing WAL for series records, we have a pointer to what is the latest segment.
		// On subsequent calls to this function, currentSegment will have been incremented and we should open that segment.
		if err := w.watch(currentSegment, currentSegment >= lastSegment); err != nil && !errors.Is(err, ErrIgnorable) {
			return err
		}

		// For testing: stop when you hit a specific segment.
		if currentSegment == w.MaxSegment {
			return nil
		}

		currentSegment++
	}

	return nil
}

// findSegmentForIndex finds the first segment greater than or equal to index.
func (w *Watcher) findSegmentForIndex(index int) (int, error) {
	refs, err := w.segments(w.walDir)
	if err != nil {
		return -1, err
	}

	for _, r := range refs {
		if r >= index {
			return r, nil
		}
	}

	return -1, errors.New("failed to find segment for index")
}

func (w *Watcher) firstAndLast() (int, int, error) {
	refs, err := w.segments(w.walDir)
	if err != nil {
		return -1, -1, err
	}

	if len(refs) == 0 {
		return -1, -1, nil
	}
	return refs[0], refs[len(refs)-1], nil
}

// Copied from tsdb/wlog/wlog.go so we do not have to open a WAL.
// Plan is to move WAL watcher to TSDB and dedupe these implementations.
func (w *Watcher) segments(dir string) ([]int, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var refs []int
	for _, f := range files {
		k, err := strconv.Atoi(f.Name())
		if err != nil {
			continue
		}
		refs = append(refs, k)
	}
	slices.Sort(refs)
	for i := 0; i < len(refs)-1; i++ {
		if refs[i]+1 != refs[i+1] {
			return nil, errors.New("segments are not sequential")
		}
	}
	return refs, nil
}

func (sr *passToWriter) readAndHandleError(w *Watcher, r *LiveReader, segmentNum int, tail bool, size int64) error {
	err := sr.readSegment(w, r, segmentNum, tail)

	// Ignore all errors reading to end of segment whilst replaying the WAL.
	if !tail {
		if err != nil && errors.Cause(err) != io.EOF {
			level.Warn(w.logger).Log("msg", "Ignoring error reading to end of segment, may have dropped data", "segment", segmentNum, "err", err)
		} else if r.Offset() != size {
			level.Warn(w.logger).Log("msg", "Expected to have read whole segment, may have dropped data", "segment", segmentNum, "read", r.Offset(), "size", size)
		}
		return ErrIgnorable
	}

	// Otherwise, when we are tailing, non-EOFs are fatal.
	if errors.Cause(err) != io.EOF {
		return err
	}
	return nil
}

// Use tail true to indicate that the reader is currently on a segment that is
// actively being written to. If false, assume it's a full segment and we're
// replaying it on start to cache the series records.
func (w *Watcher) watch(segmentNum int, tail bool) error {
	segment, err := OpenReadSegment(SegmentName(w.walDir, segmentNum))
	if err != nil {
		return err
	}
	defer segment.Close()

	reader := NewLiveReader(w.logger, w.readerMetrics, segment)

	readTicker := time.NewTicker(readTimeout)
	defer readTicker.Stop()

	checkpointTicker := time.NewTicker(checkpointPeriod)
	defer checkpointTicker.Stop()

	segmentTicker := time.NewTicker(segmentCheckPeriod)
	defer segmentTicker.Stop()

	// If we're replaying the segment we need to know the size of the file to know
	// when to return from watch and move on to the next segment.
	size := int64(math.MaxInt64)
	if !tail {
		segmentTicker.Stop()
		checkpointTicker.Stop()
		var err error
		size, err = getSegmentSize(w.walDir, segmentNum)
		if err != nil {
			return errors.Wrap(err, "getSegmentSize")
		}
	}

	segmentReader := newPassToWriter()
	gcSem := make(chan struct{}, 1)
	for {
		select {
		case <-w.quit:
			return nil

		case <-checkpointTicker.C:
			// Periodically check if there is a new checkpoint so we can garbage
			// collect labels. As this is considered an optimisation, we ignore
			// errors during checkpoint processing. Doing the process asynchronously
			// allows the current WAL segment to be processed while reading the
			// checkpoint.
			select {
			case gcSem <- struct{}{}:
				go func() {
					defer func() {
						<-gcSem
					}()
					if err := w.garbageCollectSeries(segmentNum); err != nil {
						level.Warn(w.logger).Log("msg", "Error process checkpoint", "err", err)
					}
				}()
			default:
				// Currently doing a garbage collect, try again later.
			}

		case <-segmentTicker.C:
			_, last, err := w.firstAndLast()
			if err != nil {
				return errors.Wrap(err, "segments")
			}

			// Check if new segments exists.
			if last <= segmentNum {
				continue
			}
			err = segmentReader.readSegment(w, reader, segmentNum, tail)

			// Ignore errors reading to end of segment whilst replaying the WAL.
			if !tail {
				switch {
				case err != nil && errors.Cause(err) != io.EOF:
					level.Warn(w.logger).Log("msg", "Ignoring error reading to end of segment, may have dropped data", "err", err)
				case reader.Offset() != size:
					level.Warn(w.logger).Log("msg", "Expected to have read whole segment, may have dropped data", "segment", segmentNum, "read", reader.Offset(), "size", size)
				}
				return nil
			}

			// Otherwise, when we are tailing, non-EOFs are fatal.
			if errors.Cause(err) != io.EOF {
				return err
			}

			return nil

		// we haven't read due to a notification in quite some time, try reading anyways
		case <-readTicker.C:
			level.Debug(w.logger).Log("msg", "Watcher is reading the WAL due to timeout, haven't received any write notifications recently", "timeout", readTimeout)
			err := segmentReader.readAndHandleError(w, reader, segmentNum, tail, size)
			if err != nil {
				return err
			}
			// still want to reset the ticker so we don't read too often
			readTicker.Reset(readTimeout)

		case <-w.readNotify:
			err := segmentReader.readAndHandleError(w, reader, segmentNum, tail, size)
			if err != nil {
				return err
			}
			// still want to reset the ticker so we don't read too often
			readTicker.Reset(readTimeout)
		}
	}
}

func (w *Watcher) garbageCollectSeries(segmentNum int) error {
	dir, _, err := LastCheckpoint(w.walDir)
	if err != nil && err != record.ErrNotFound {
		return errors.Wrap(err, "tsdb.LastCheckpoint")
	}

	if dir == "" || dir == w.lastCheckpoint {
		return nil
	}
	w.lastCheckpoint = dir

	index, err := checkpointNum(dir)
	if err != nil {
		return errors.Wrap(err, "error parsing checkpoint filename")
	}

	if index >= segmentNum {
		level.Info(w.logger).Log("msg", "Current segment is behind the checkpoint, skipping reading of checkpoint", "current", fmt.Sprintf("%08d", segmentNum), "checkpoint", dir)
		return nil
	}

	level.Info(w.logger).Log("msg", "New checkpoint detected", "last", w.lastCheckpoint, "new", dir, "index", index, "currentSegment", segmentNum)

	if err = w.readCheckpoint(dir, &segmentReaderForGC{}); err != nil {
		return errors.Wrap(err, "readCheckpoint")
	}

	// Clear series with a checkpoint or segment index # lower than the checkpoint we just read.
	w.writer.SeriesReset(index)
	return nil
}

type segmentReader interface {
	readSegment(w *Watcher, r *LiveReader, segmentNum int, tail bool) error
}

// Data structures that we want to re-use across multiple segment reads, to save garbage.
type passToWriter struct {
	dec                   record.Decoder
	series                []record.RefSeries
	samples               []record.RefSample
	samplesToSend         []record.RefSample
	exemplars             []record.RefExemplar
	histograms            []record.RefHistogramSample
	histogramsToSend      []record.RefHistogramSample
	floatHistograms       []record.RefFloatHistogramSample
	floatHistogramsToSend []record.RefFloatHistogramSample
}

func newPassToWriter() *passToWriter {
	return &passToWriter{
		dec: record.NewDecoder(labels.NewSymbolTable()), // TODO: should this symboltable be linked with something else?
	}
}

// Read from a segment and pass the details to w.writer.
// Also used with readCheckpoint - implements segmentReadFn.
func (sr *passToWriter) readSegment(w *Watcher, r *LiveReader, segmentNum int, tail bool) error {
	sr.samplesToSend = sr.samplesToSend[:0]
	sr.histogramsToSend = sr.histogramsToSend[:0]
	sr.floatHistogramsToSend = sr.floatHistogramsToSend[:0]

	for r.Next() && !isClosed(w.quit) {
		rec := r.Record()
		w.recordsReadMetric.WithLabelValues(sr.dec.Type(rec).String()).Inc()
		var err error

		switch sr.dec.Type(rec) {
		case record.Series:
			sr.series, err = sr.dec.Series(rec, sr.series[:0])
			if err != nil {
				w.recordDecodeFailsMetric.Inc()
				return err
			}
			w.writer.StoreSeries(sr.series, segmentNum)

		case record.Samples:
			// If we're not tailing a segment we can ignore any samples records we see.
			// This speeds up replay of the WAL by > 10x.
			if !tail {
				break
			}
			sr.samples, err = sr.dec.Samples(rec, sr.samples[:0])
			if err != nil {
				w.recordDecodeFailsMetric.Inc()
				return err
			}
			for _, s := range sr.samples {
				if s.T > w.startTimestamp {
					if !w.sendSamples {
						w.sendSamples = true
						duration := time.Since(w.startTime)
						level.Info(w.logger).Log("msg", "Done replaying WAL", "duration", duration)
					}
					sr.samplesToSend = append(sr.samplesToSend, s)
				}
			}
			if len(sr.samplesToSend) > 0 {
				w.writer.Append(sr.samplesToSend)
				sr.samplesToSend = sr.samplesToSend[:0]
			}

		case record.Exemplars:
			// Skip if experimental "exemplars over remote write" is not enabled.
			if !w.sendExemplars {
				break
			}
			// If we're not tailing a segment we can ignore any exemplars records we see.
			// This speeds up replay of the WAL significantly.
			if !tail {
				break
			}
			sr.exemplars, err = sr.dec.Exemplars(rec, sr.exemplars[:0])
			if err != nil {
				w.recordDecodeFailsMetric.Inc()
				return err
			}
			w.writer.AppendExemplars(sr.exemplars)

		case record.HistogramSamples:
			// Skip if experimental "histograms over remote write" is not enabled.
			if !w.sendHistograms {
				break
			}
			if !tail {
				break
			}
			sr.histograms, err = sr.dec.HistogramSamples(rec, sr.histograms[:0])
			if err != nil {
				w.recordDecodeFailsMetric.Inc()
				return err
			}
			for _, h := range sr.histograms {
				if h.T > w.startTimestamp {
					if !w.sendSamples {
						w.sendSamples = true
						duration := time.Since(w.startTime)
						level.Info(w.logger).Log("msg", "Done replaying WAL", "duration", duration)
					}
					sr.histogramsToSend = append(sr.histogramsToSend, h)
				}
			}
			if len(sr.histogramsToSend) > 0 {
				w.writer.AppendHistograms(sr.histogramsToSend)
				sr.histogramsToSend = sr.histogramsToSend[:0]
			}
		case record.FloatHistogramSamples:
			// Skip if experimental "histograms over remote write" is not enabled.
			if !w.sendHistograms {
				break
			}
			if !tail {
				break
			}
			sr.floatHistograms, err = sr.dec.FloatHistogramSamples(rec, sr.floatHistograms[:0])
			if err != nil {
				w.recordDecodeFailsMetric.Inc()
				return err
			}
			for _, fh := range sr.floatHistograms {
				if fh.T > w.startTimestamp {
					if !w.sendSamples {
						w.sendSamples = true
						duration := time.Since(w.startTime)
						level.Info(w.logger).Log("msg", "Done replaying WAL", "duration", duration)
					}
					sr.floatHistogramsToSend = append(sr.floatHistogramsToSend, fh)
				}
			}
			if len(sr.floatHistogramsToSend) > 0 {
				w.writer.AppendFloatHistograms(sr.floatHistogramsToSend)
				sr.floatHistogramsToSend = sr.floatHistogramsToSend[:0]
			}
		case record.Tombstones:

		default:
			// Could be corruption, or reading from a WAL from a newer Prometheus.
			w.recordDecodeFailsMetric.Inc()
		}
	}
	return errors.Wrapf(r.Err(), "segment %d: %v", segmentNum, r.Err())
}

type segmentReaderForGC struct {
}

// Go through all series in a segment updating the segmentNum, so we can delete older series.
// Used with readCheckpoint - implements segmentReadFn.
func (segmentReaderForGC) readSegment(w *Watcher, r *LiveReader, segmentNum int, _ bool) error {
	var (
		dec    = record.NewDecoder(labels.NewSymbolTable()) // TODO: should this symboltable be linked with something else?
		series []record.RefSeries
	)
	for r.Next() && !isClosed(w.quit) {
		rec := r.Record()
		w.recordsReadMetric.WithLabelValues(dec.Type(rec).String()).Inc()

		switch dec.Type(rec) {
		case record.Series:
			series, err := dec.Series(rec, series[:0]) // TODO: we don't need the labels; see if we can save work.
			if err != nil {
				w.recordDecodeFailsMetric.Inc()
				return err
			}
			w.writer.UpdateSeriesSegment(series, segmentNum)

		// Ignore these; we're only interested in series.
		case record.Samples:
		case record.Exemplars:
		case record.Tombstones:

		default:
			// Could be corruption, or reading from a WAL from a newer Prometheus.
			w.recordDecodeFailsMetric.Inc()
		}
	}
	return errors.Wrapf(r.Err(), "segment %d: %v", segmentNum, r.Err())
}

func (w *Watcher) SetStartTime(t time.Time) {
	w.startTime = t
	w.startTimestamp = timestamp.FromTime(t)
}

// Read all the series records from a Checkpoint directory.
func (w *Watcher) readCheckpoint(checkpointDir string, segmentReader segmentReader) error {
	level.Debug(w.logger).Log("msg", "Reading checkpoint", "dir", checkpointDir)
	index, err := checkpointNum(checkpointDir)
	if err != nil {
		return errors.Wrap(err, "checkpointNum")
	}

	// Ensure we read the whole contents of every segment in the checkpoint dir.
	segs, err := w.segments(checkpointDir)
	if err != nil {
		return errors.Wrap(err, "Unable to get segments checkpoint dir")
	}
	for _, seg := range segs {
		size, err := getSegmentSize(checkpointDir, seg)
		if err != nil {
			return errors.Wrap(err, "getSegmentSize")
		}

		sr, err := OpenReadSegment(SegmentName(checkpointDir, seg))
		if err != nil {
			return errors.Wrap(err, "unable to open segment")
		}
		defer sr.Close()

		r := NewLiveReader(w.logger, w.readerMetrics, sr)
		if err := segmentReader.readSegment(w, r, index, false); errors.Cause(err) != io.EOF && err != nil {
			return errors.Wrap(err, "readSegment")
		}

		if r.Offset() != size {
			return fmt.Errorf("readCheckpoint wasn't able to read all data from the checkpoint %s/%08d, size: %d, totalRead: %d", checkpointDir, seg, size, r.Offset())
		}
	}

	level.Debug(w.logger).Log("msg", "Read series references from checkpoint", "checkpoint", checkpointDir)
	return nil
}

func checkpointNum(dir string) (int, error) {
	// Checkpoint dir names are in the format checkpoint.000001
	// dir may contain a hidden directory, so only check the base directory
	chunks := strings.Split(filepath.Base(dir), ".")
	if len(chunks) != 2 {
		return 0, errors.Errorf("invalid checkpoint dir string: %s", dir)
	}

	result, err := strconv.Atoi(chunks[1])
	if err != nil {
		return 0, errors.Errorf("invalid checkpoint dir string: %s", dir)
	}

	return result, nil
}

// Get size of segment.
func getSegmentSize(dir string, index int) (int64, error) {
	i := int64(-1)
	fi, err := os.Stat(SegmentName(dir, index))
	if err == nil {
		i = fi.Size()
	}
	return i, err
}

func isClosed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
