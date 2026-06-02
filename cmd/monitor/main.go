package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"kafka-taskflow/internal/monitor"
	"kafka-taskflow/internal/queue"

	"github.com/segmentio/kafka-go"
)

type snapshotResponse struct {
	Topics   []string                   `json:"topics"`
	Messages map[string][]monitor.Event `json:"messages"`
}

func getTopicData(ctx context.Context, broker, topic, group string, hub *monitor.Hub, wg *sync.WaitGroup) {
	defer wg.Done()

	c := queue.NewConsumer(broker, topic, group)
	defer c.Close()

	var lastErrLog time.Time
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := c.Fetch(fetchCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if time.Since(lastErrLog) > 10*time.Second {
				log.Printf("monitor consumer %q fetch error: %v", topic, err)
				lastErrLog = time.Now()
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		hub.Publish(eventFromMessage(topic, msg))

		commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := c.Commit(commitCtx, msg); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("monitor consumer %q commit error: %v", topic, err)
		}
		cancel()
	}
}

func eventFromMessage(topic string, msg *kafka.Message) monitor.Event {
	payload := json.RawMessage(append([]byte(nil), msg.Value...))
	if !json.Valid(payload) {
		encoded, _ := json.Marshal(string(msg.Value))
		payload = encoded
	}

	return monitor.Event{
		Topic:     topic,
		Payload:   payload,
		Received:  time.Now().UTC(),
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       string(msg.Key),
	}
}

func sseHandler(hub *monitor.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		ch, unsub := hub.Subscribe()
		defer unsub()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		fmt.Fprintf(w, ": connected\n\n")
		flusher.Flush()

		heartbeat := time.NewTicker(25 * time.Second)
		defer heartbeat.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-heartbeat.C:
				fmt.Fprintf(w, ": heartbeat %s\n\n", time.Now().UTC().Format(time.RFC3339))
				flusher.Flush()
			case event, ok := <-ch:
				if !ok {
					return
				}
				data, err := json.Marshal(event)
				if err != nil {
					log.Printf("monitor marshal event error: %v", err)
					continue
				}
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
	}
}

func topicsHandler(hub *monitor.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, hub.Topics())
	}
}

func snapshotHandler(hub *monitor.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, snapshotResponse{
			Topics:   hub.Topics(),
			Messages: hub.Snapshot(),
		})
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("monitor response encode error: %v", err)
	}
}

func main() {
	defaultTopics := strings.Join(queue.AllTopics(), ",")

	fmt.Printf("http://localhost:8080\n")

	var (
		broker       = flag.String("broker", "localhost:9092", "kafka broker address")
		topicsFlag   = flag.String("topics", defaultTopics, "comma-separated kafka topics to consume")
		group        = flag.String("group", "monitor-group", "consumer group id")
		addr         = flag.String("addr", ":8080", "http listen address")
		historyLimit = flag.Int("history", monitor.DefaultHistoryLimit, "messages retained per topic for browser snapshots")
	)
	flag.Parse()

	topics := splitTopics(*topicsFlag)
	hub := monitor.NewHub(topics, *historyLimit)

	consumerCtx, cancelConsumers := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, topic := range topics {
		wg.Add(1)
		go getTopicData(consumerCtx, *broker, topic, *group, hub, &wg)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/events", sseHandler(hub))
	mux.HandleFunc("/api/topics", topicsHandler(hub))
	mux.HandleFunc("/api/snapshot", snapshotHandler(hub))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	webDir := findStaticDir()
	mux.Handle("/", http.FileServer(http.Dir(webDir)))

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		cancelConsumers()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("monitor shutdown error: %v", err)
		}
	}()

	log.Printf("monitor serving %s from %s", *addr, webDir)
	log.Printf("monitor consuming %s via %s", strings.Join(topics, ", "), *broker)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		cancelConsumers()
		log.Fatalf("monitor server error: %v", err)
	}

	cancelConsumers()
	wg.Wait()
}

func findStaticDir() string {
	candidates := []string{}
	if dir := strings.TrimSpace(os.Getenv("MONITOR_WEB_DIR")); dir != "" {
		candidates = append(candidates, dir)
	}

	candidates = append(candidates, "web/monitor", "../../web/monitor")

	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(exeDir, "web/monitor"),
			filepath.Join(exeDir, "../../web/monitor"),
		)
	}

	if _, file, _, ok := runtime.Caller(0); ok {
		repoRoot := filepath.Join(filepath.Dir(file), "../..")
		candidates = append(candidates, filepath.Join(repoRoot, "web/monitor"))
	}

	for _, dir := range candidates {
		if dir == "" {
			continue
		}
		if info, err := os.Stat(dir); err == nil && info.IsDir() {
			return dir
		}
	}

	return "web/monitor"
}

func splitTopics(s string) []string {
	seen := make(map[string]struct{})
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))

	for _, p := range parts {
		topic := strings.TrimSpace(p)
		if topic == "" {
			continue
		}
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		out = append(out, topic)
	}

	return out
}
