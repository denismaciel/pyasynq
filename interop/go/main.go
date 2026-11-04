// Command interop drives real Go asynq so pyasynq tests can check wire
// compatibility in both directions.
//
//	interop enqueue -addr localhost:6379           # prints the TaskInfo as JSON
//	interop inspect -addr localhost:6379 -queue q -id ID
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
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
	NextAt   int64             `json:"next_process_at"`
}

func toJSON(info *asynq.TaskInfo) taskJSON {
	var deadline, nextAt int64
	if !info.Deadline.IsZero() {
		deadline = info.Deadline.Unix()
	}
	if !info.NextProcessAt.IsZero() {
		nextAt = info.NextProcessAt.Unix()
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
		NextAt:   nextAt,
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
	typename := fs.String("type", "email:send", "task type")
	payload := fs.String("payload", `{"to":"denis@example.com"}`, "task payload")
	unique := fs.Duration("unique", 0, "uniqueness TTL, e.g. 1h")
	group := fs.String("group", "", "group key")
	duration := fs.Duration("duration", 2*time.Second, "how long the worker runs")
	entry := fs.String("entry", "", "scheduler entry id")
	fs.Parse(os.Args[2:])
	redisOpt := asynq.RedisClientOpt{Addr: *addr}

	var info *asynq.TaskInfo
	var err error
	switch os.Args[1] {
	case "enqueue":
		client := asynq.NewClient(redisOpt)
		defer client.Close()
		task := asynq.NewTaskWithHeaders(
			*typename,
			[]byte(*payload),
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
		if *unique > 0 {
			opts = append(opts, asynq.Unique(*unique))
		}
		if *group != "" {
			opts = append(opts, asynq.Group(*group))
		}
		info, err = client.Enqueue(task, opts...)
	case "servers":
		inspector := asynq.NewInspector(redisOpt)
		defer inspector.Close()
		servers, err := inspector.Servers()
		emit(servers, err)
		return
	case "cancel":
		inspector := asynq.NewInspector(redisOpt)
		defer inspector.Close()
		emit(map[string]bool{"ok": true}, inspector.CancelProcessing(*id))
		return
	case "schedulers":
		inspector := asynq.NewInspector(redisOpt)
		defer inspector.Close()
		entries, err := inspector.SchedulerEntries()
		emit(entries, err)
		return
	case "history":
		inspector := asynq.NewInspector(redisOpt)
		defer inspector.Close()
		events, err := inspector.ListSchedulerEnqueueEvents(*entry)
		emit(events, err)
		return
	case "worker":
		runWorker(redisOpt, *queue, *duration)
		return
	case "inspect":
		inspector := asynq.NewInspector(redisOpt)
		defer inspector.Close()
		info, err = inspector.GetTaskInfo(*queue, *id)
	case "pause":
		inspector := asynq.NewInspector(redisOpt)
		defer inspector.Close()
		if err := inspector.PauseQueue(*queue); err != nil {
			json.NewEncoder(os.Stdout).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(os.Stdout).Encode(map[string]bool{"paused": true})
		return
	case "queue":
		inspector := asynq.NewInspector(redisOpt)
		defer inspector.Close()
		qinfo, err := inspector.GetQueueInfo(*queue)
		if err != nil {
			json.NewEncoder(os.Stdout).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(os.Stdout).Encode(qinfo)
		return
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		json.NewEncoder(os.Stdout).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(os.Stdout).Encode(toJSON(info))
}

func emit(v any, err error) {
	if err != nil {
		v = map[string]string{"error": err.Error()}
	}
	json.NewEncoder(os.Stdout).Encode(v)
}

type handled struct {
	ID      string            `json:"id"`
	Type    string            `json:"type"`
	Payload []byte            `json:"payload"`
	Headers map[string]string `json:"headers"`
}

// runWorker processes every task in queue for the given duration with a
// Go asynq server, failing tasks whose payload is "fail", and prints what
// it handled.
func runWorker(redisOpt asynq.RedisClientOpt, queue string, d time.Duration) {
	var mu sync.Mutex
	var seen []handled
	srv := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: 4,
		Queues:      map[string]int{queue: 1},
		Logger:      quietLogger{},
		LogLevel:    asynq.ErrorLevel,
	})
	h := asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		id, _ := asynq.GetTaskID(ctx)
		mu.Lock()
		seen = append(seen, handled{id, t.Type(), t.Payload(), t.Headers()})
		mu.Unlock()
		if string(t.Payload()) == "fail" {
			return fmt.Errorf("go handler failed")
		}
		return nil
	})
	if err := srv.Start(h); err != nil {
		emit(nil, err)
		return
	}
	time.Sleep(d)
	srv.Shutdown()
	mu.Lock()
	defer mu.Unlock()
	if seen == nil {
		seen = []handled{}
	}
	emit(seen, nil)
}

type quietLogger struct{}

func (quietLogger) Debug(args ...any) {}
func (quietLogger) Info(args ...any)  {}
func (quietLogger) Warn(args ...any)  {}
func (quietLogger) Error(args ...any) {}
func (quietLogger) Fatal(args ...any) { os.Exit(1) }
