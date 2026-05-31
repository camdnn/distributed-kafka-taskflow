// gets asdb data and published it to a topic in the broker
package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"

	"example.com/m/internal/task"
)

const (
	SampleDataPath = "../../data/sample-adsb.ndjson"
)

func main() {
	// var tasks []task.Task = getSampleData()
	getSampleData()

}

// func getAdsbData() []task.Task {

// }

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
