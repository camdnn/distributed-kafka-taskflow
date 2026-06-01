// gets asdb data and published it to a topic in the broker
package main

import (
	"bufio"
	"bytes"
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
	var payloadMap map[string]any
	if err := json.Unmarshal(t.Payload, &payloadMap); err == nil {
		if hexVal, ok := payloadMap["hex"].(string); ok && len(hexVal) > 0 {
			key = []byte(hexVal)
		}
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

	var rawMsgs []json.RawMessage

	// 1) Try wrapper {"data": [...]}
	var wrapper struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapper); err == nil && len(wrapper.Data) > 0 {
		rawMsgs = wrapper.Data
	} else if err := json.Unmarshal(body, &rawMsgs); err == nil && len(rawMsgs) > 0 {
		// top-level array parsed successfully
	} else {
		// fall back to ndjson (one JSON object per line)
		scanner := bufio.NewScanner(bytes.NewReader(body))
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			// copy bytes into a new slice
			rawMsgs = append(rawMsgs, append([]byte(nil), line...))
		}
		if err := scanner.Err(); err != nil {
			log.Printf("failed to parse adsb response as ndjson: %v", err)
			return nil
		}
	}

	var res []task.Task
	for _, m := range rawMsgs {
		t := task.NewTask("plane", m)
		res = append(res, *t)
	}

	return res

}

//
// // used for testing with fixed data
// func getSampleData() []task.Task {
// 	file, err := os.Open(SampleDataPath)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()
//
// 	var res []task.Task
//
// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		line := scanner.Bytes()
//
// 		if len(line) == 0 {
// 			continue
// 		}
//
// 		payload := json.RawMessage(append([]byte(nil), line...))
// 		t := task.NewTask("plane", payload)
//
// 		t.Display()
//
// 		res = append(res, *t)
//
// 	}
//
// 	if err := scanner.Err(); err != nil {
// 		log.Fatal(err)
// 	}
//
// 	return res
// }
