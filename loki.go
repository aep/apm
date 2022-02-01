//go:generate protoc -I . --go_out=. logproto.proto
package apm

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"
)

const (
	contentType  = "application/x-protobuf"
	postPath     = "/api/prom/push"
	maxErrMsgLen = 1024
)

type lokientry struct {
	labels model.LabelSet
	*Entry
}

type Loki struct {
	yeet        chan lokientry
	hostname    string

    LokiConfig  *LokiConfig
    AppConfig  *AppConfig
}

type AppConfig struct {
    Name    string
}

type LokiConfig struct {
    URL         string

    MaxBatchSize    int
    MaxBatchDelay   time.Duration
}


func NewLogger(AppConfig *AppConfig, LokiConfig*LokiConfig) *Loki {

	hostname, _ := os.Hostname()

    if LokiConfig.MaxBatchSize < 1 {
        LokiConfig.MaxBatchSize = 100
    }
    if LokiConfig.MaxBatchDelay < 300 * time.Millisecond {
        LokiConfig.MaxBatchDelay = 300 * time.Millisecond
    }

	l := &Loki{
        LokiConfig: LokiConfig,
        AppConfig:  AppConfig,
		yeet:       make(chan lokientry, 100),
		hostname:  hostname,
	}

	u, err := url.Parse(LokiConfig.URL)
    if err != nil { fmt.Fprintln(os.Stderr, err)}

	if !strings.Contains(u.Path, postPath) {
		u.Path = postPath
		q := u.Query()
		u.RawQuery = q.Encode()
		LokiConfig.URL = u.String()
	}
	go l.run()
	return l
}

func (self *Loki) Fire(entry *logrus.Entry) error {

    var l lokientry

    tsNano := entry.Time.UnixNano()
    ts := &timestamp.Timestamp{
        Seconds: tsNano / int64(time.Second),
        Nanos:   int32(tsNano % int64(time.Second)),
    }
    l = lokientry{model.LabelSet{}, &Entry{Timestamp: ts}}
    l.labels["appname"]   = model.LabelValue(self.AppConfig.Name)
    l.labels["level"]     = model.LabelValue(entry.Level.String())
    l.labels["hostname"]  = model.LabelValue(self.hostname)

    for key, value := range entry.Data {
        l.labels[model.LabelName(key)] = model.LabelValue(fmt.Sprintf("%v", value))
    }

    var err error
    l.Entry.Line, err = entry.String()
    if err != nil {
        fmt.Fprintf(os.Stderr, "ERROR: logrus format error: %v\n", err)
    }

    select {
        case  self.yeet <- l:
        default:
    }
    return nil
}


func (l *Loki) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (l *Loki) run() {

    var backlog     = 0
    var batch       = map[model.Fingerprint]*Stream{}
    var batchAge    = time.Now()

    for ;; {
        var sendNow = false

        timeout := time.Second
        if backlog > 0 {
            timeout = time.Until(batchAge.Add(l.LokiConfig.MaxBatchDelay))
            if timeout < 0 {
                timeout = 0
                sendNow = true
            }
        }

        select {
            case <- time.After(timeout):
                sendNow = true
            case entry,ok := <- l.yeet:
                if !ok {
                    return
                }
                fp := entry.labels.FastFingerprint()
                stream, ok := batch[fp]
                if !ok {
                    stream = &Stream{
                        Labels: entry.labels.String(),
                    }
                    batch[fp] = stream
                }
                stream.Entries = append(stream.Entries, entry.Entry)
                if backlog == 0 {
                    batchAge = time.Now()
                }
                backlog += 1
        }

        if sendNow && backlog > 0 || backlog >= l.LokiConfig.MaxBatchSize {
            if err := l.sendBatch(batch); err != nil {
                fmt.Fprintln(os.Stderr, "loki", err)
            }
            batch = map[model.Fingerprint]*Stream{}
            backlog = 0
        }
	}
}

func (l *Loki) sendBatch(batch map[model.Fingerprint]*Stream) error {
	buf, err := encodeBatch(batch)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = l.send(ctx, buf)
	if err != nil {
		return err
	}
	return nil
}

func encodeBatch(batch map[model.Fingerprint]*Stream) ([]byte, error) {
	req := PushRequest{
		Streams: make([]*Stream, 0, len(batch)),
	}
	for _, stream := range batch {
		req.Streams = append(req.Streams, stream)
	}
	buf, err := proto.Marshal(&req)
	if err != nil {
		return nil, err
	}
	buf = snappy.Encode(nil, buf)
	return buf, nil
}

func (l *Loki) send(ctx context.Context, buf []byte) (int, error) {

	req, err := http.NewRequest("POST", l.LokiConfig.URL, bytes.NewReader(buf))
	if err != nil {
		return -1, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", contentType)

    var hc = &http.Client{
        Timeout: time.Second * 1,
    }

	resp, err := hc.Do(req)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s (%d): %s", resp.Status, resp.StatusCode, line)
	}
	return resp.StatusCode, err
}
