// gets asdb data and published it to a topic in the broker
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"kafka-taskflow/internal/queue"
	"kafka-taskflow/internal/task"
	"log"
	"net/http"
	"time"
)

const (
	SampleDataPath = "../../data/sample-adsb.ndjson"
	AdsbAPIURL     = "https://api.adsb.lol/v2/point/33.942608/-118.418406/15"
)

func main() {
	// get fresh data and publish forever
	p := queue.NewProducer("localhost:9092")
	defer p.Close()

	for {
		var tasks = getAdsbData()

		for _, t := range tasks {

			if err := publishToTaskTopic(p, t); err != nil {
				log.Printf("publish error: %v", err)
				continue
			}
			log.Printf("published task %s", t.ID)
		}
	}

}

// send all new tasks to the task topic in the kafka broker
func publishToTaskTopic(p *queue.Producer, t task.Task) error {
	// serialize task; fall back if payload contains invalid JSON
	value, err := t.ToJSON()
	if err != nil {
		// fallback builder
		var payloadPart []byte
		if json.Valid(t.Payload) {
			payloadPart = t.Payload
		} else {
			// embed as JSON string to avoid invalid JSON in the message
			str, _ := json.Marshal(string(t.Payload))
			payloadPart = str
		}
		// build minimal JSON object
		value = fmt.Appendf(value, `{"id":"%s","type":"%s","payload":%s}`, t.ID, t.Type, payloadPart)
	}

	// try to pick a stable key (prefer payload.hex when present)
	key := []byte(t.ID)
	if a, err := task.AircraftFromJSON(t.Payload); err == nil && a.Hex != "" {
		key = []byte(a.Hex)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return p.Publish(ctx, queue.TopicTask, key, value)
}

// using the adsb api url
func getAdsbData() []task.Task {

	resp, err := http.Get(AdsbAPIURL)
	if err != nil {
		log.Printf("failed to fetch ADSB data: %v", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("adsb api returned non-200: %s", resp.Status)
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("failed to read adsb response: %v", err)
		return nil
	}

	var wrapper struct {
		Now   float64           `json:"now"`
		Ac    []json.RawMessage `json:"ac"`
		Total int               `json:"total"`
	}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		log.Printf("failed to parse asdb data :%v", err)
		return nil
	}

	// responseTime: convert now (ms) once, outside the loop — same for all aircraft in this batch
	responseTime := time.UnixMilli(int64(wrapper.Now))

	// setup task
	var res []task.Task
	for _, m := range wrapper.Ac {

		a, err := task.AircraftFromJSON(m)

		if err != nil {
			log.Printf("skip malformed data: %v", err)
			continue
		}

		// compute observation time = responseTime - seen
		if a.Seen != nil {
			a.ObservedAt = responseTime.Add(-time.Duration(*a.Seen * float64(time.Second)))
		} else {
			a.ObservedAt = responseTime // no seen value -> best effort, use response time
		}

		// re-serialize the enriched aircraft as the task payload
		payload, err := a.ToJSON()
		if err != nil {
			log.Printf("skip aircraft, serialize failed: %v", err)
			continue
		}

		// add payload back into data
		t := task.NewTask("process_aircraft", payload)
		res = append(res, *t)
	}

	return res

}

// used for testing with fixed data
func getSampleData() []task.Task {
	file, err := os.Open(SampleDataPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var res []task.Task

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()

		if len(line) == 0 {
			continue
		}

		payload := json.RawMessage(append([]byte(nil), line...))
		t := task.NewTask("plane", payload)

		t.Display()

		res = append(res, *t)

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return res
}
