// Command interop drives real Go asynq so pyasynq tests can check wire
// compatibility in both directions.
//
//	interop enqueue -addr localhost:6379           # prints the TaskInfo as JSON
//	interop inspect -addr localhost:6379 -queue q -id ID
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/hibiken/asynq"
)

type taskJSON struct {
	ID       string            `json:"id"`
	Queue    string            `json:"queue"`
	Type     string            `json:"type"`
	Payload  []byte            `json:"payload"`
	Headers  map[string]string `json:"headers"`
	State    string            `json:"state"`
	MaxRetry int               `json:"max_retry"`
	Retried  int               `json:"retried"`
	LastErr  string            `json:"last_err"`
	Timeout  float64           `json:"timeout_seconds"`
	Deadline int64             `json:"deadline"`
	Group    string            `json:"group"`
	Retain   float64           `json:"retention_seconds"`
}

func toJSON(info *asynq.TaskInfo) taskJSON {
	var deadline int64
	if !info.Deadline.IsZero() {
		deadline = info.Deadline.Unix()
	}
	return taskJSON{
		ID:       info.ID,
		Queue:    info.Queue,
		Type:     info.Type,
		Payload:  info.Payload,
		Headers:  info.Headers,
		State:    info.State.String(),
		MaxRetry: info.MaxRetry,
		Retried:  info.Retried,
		LastErr:  info.LastErr,
		Timeout:  info.Timeout.Seconds(),
		Deadline: deadline,
		Group:    info.Group,
		Retain:   info.Retention.Seconds(),
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: interop <enqueue|inspect> [flags]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	addr := fs.String("addr", "localhost:6379", "redis address")
	queue := fs.String("queue", "critical", "queue name")
	id := fs.String("id", "", "task id")
	fs.Parse(os.Args[2:])
	redisOpt := asynq.RedisClientOpt{Addr: *addr}

	var info *asynq.TaskInfo
	var err error
	switch os.Args[1] {
	case "enqueue":
		client := asynq.NewClient(redisOpt)
		defer client.Close()
		task := asynq.NewTaskWithHeaders(
			"email:send",
			[]byte(`{"to":"denis@example.com"}`),
			map[string]string{"content-type": "application/json"},
		)
		opts := []asynq.Option{
			asynq.Queue(*queue),
			asynq.MaxRetry(7),
			asynq.Timeout(90 * time.Second),
			asynq.Retention(time.Hour),
		}
		if *id != "" {
			opts = append(opts, asynq.TaskID(*id))
		}
		info, err = client.Enqueue(task, opts...)
	case "inspect":
		inspector := asynq.NewInspector(redisOpt)
		defer inspector.Close()
		info, err = inspector.GetTaskInfo(*queue, *id)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	json.NewEncoder(os.Stdout).Encode(toJSON(info))
}
