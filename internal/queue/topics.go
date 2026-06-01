package queue

const (
	TopicTask       = "tasks"
	TopicRetries    = "retries"
	TopicDeadLetter = "dead-letter"
	TopicResults    = "results"
	TopicTaskStatus = "task-status"
	TopicAlerts     = "alerts"

	TopicStatus = "task-status"
)

func AllTopics() []string {
	return []string{
		TopicTask,
		TopicRetries,
		TopicDeadLetter,
		TopicResults,
		TopicTaskStatus,
		TopicAlerts,
	}
}
