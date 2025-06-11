package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/saurabhsingh121/distributed-scheduler/pkg/common"
)

// Command Request Represent the structure of the command request
type CommandRequest struct {
	Command    string `json:"command"`
	ScheduleAt string `json:"schedule_at"` // ISO 8601 format
}

type Task struct {
	ID          string
	Command     string
	ScheduleAt  pgtype.Timestamp
	PickedAt    pgtype.Timestamp
	StartedAt   pgtype.Timestamp
	CompletedAt pgtype.Timestamp
	FailedAt    pgtype.Timestamp
}

// ScheulerServer represents a scheduler server
type SchedulerServer struct {
	serverPort         string
	dbConnectionString string
	dbPool             *pgxpool.Pool
	ctx                context.Context
	cancel             context.CancelFunc
	httpServer         *http.Server
}

// NewServer creates a new scheduler server
func NewServer(port string, dbConnectionString string) *SchedulerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerServer{
		serverPort:         port,
		dbConnectionString: dbConnectionString,
		ctx:                ctx,
		cancel:             cancel,
	}
}

// Start initializes and starts the scheduler server
func (s *SchedulerServer) Start() error {
	var err error
	s.dbPool, err = common.ConnectToDatabase(s.ctx, s.dbConnectionString)
	if err != nil {
		return err
	}

	http.HandleFunc("/schedule", s.handleScheduleTask)
	http.HandleFunc("/status/", s.handleGetTaskStatus)

	s.httpServer = &http.Server{
		Addr: s.serverPort,
	}

	log.Printf("Starting scheduler server on %s\n", s.serverPort)

	// start the server on a separate go routine
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	// return await shutdown
	return s.awaitShutdown()
}

// hadleScheduleTask hanles POST requests to create a new task
func (s *SchedulerServer) handleScheduleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// decode the request body
	var commandRequest CommandRequest
	if err := json.NewDecoder(r.Body).Decode(&commandRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Received schedule request: %+v", commandRequest)

	// parse the scheduleAt time
	scheduleAt, err := time.Parse(time.RFC3339, commandRequest.ScheduleAt)
	if err != nil {
		http.Error(w, "Invalid time format. Expected ISO 8601", http.StatusBadRequest)
		return
	}

	// convert the schedule to unix timestamp
	unixTimestamp := time.Unix(scheduleAt.Unix(), 0)
	taskId, err := s.insertTaskIntoDB(context.Background(), Task{Command: commandRequest.Command, ScheduleAt: pgtype.Timestamp{Time: unixTimestamp}})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to submit task. Error: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	// respond with parsed data
	response := struct {
		Command    string `json:"command"`
		ScheduleAt int64  `json:"schedule_at"`
		TaskID     string `json:"task_id"`
	}{
		Command:    commandRequest.Command,
		ScheduleAt: unixTimestamp.Unix(),
		TaskID:     taskId,
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal response. Error: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)
	log.Printf("Scheduled task %s for %s", taskId, commandRequest.Command)
}

// handleGetTaskStatus handles GET requests to retreive the status of a task
func (s *SchedulerServer) handleGetTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// get the task ID from the query params
	taskID := r.URL.Query().Get("task_id")

	// check if task id is empty
	if taskID == "" {
		http.Error(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	// query the database to get the task status
	var task Task
	err := s.dbPool.QueryRow(context.Background(), "SELECT * from task WHERE id = $1", taskID).Scan(
		&task.ID,
		&task.Command,
		&task.ScheduleAt,
		&task.PickedAt,
		&task.StartedAt,
		&task.CompletedAt,
		&task.FailedAt,
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get task status. Error: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	// prepare the response
	response := struct {
		TaskID      string `json:"task_id"`
		Command     string `json:"command"`
		ScheduleAt  string `json:"schedule_at"`
		PickedAt    string `json:"picked_at"`
		StartedAt   string `json:"started_at"`
		CompletedAt string `json:"completed_at"`
		FailedAt    string `json:"failed_at"`
	}{
		TaskID:      task.ID,
		Command:     task.Command,
		ScheduleAt:  "",
		PickedAt:    "",
		StartedAt:   "",
		CompletedAt: "",
		FailedAt:    "",
	}

	// set the scheduleAt time if not null
	if task.ScheduleAt.Status == pgtype.Present {
		response.ScheduleAt = task.ScheduleAt.Time.String()
	}

	// set the pickedAt time if not null
	if task.PickedAt.Status == pgtype.Present {
		response.PickedAt = task.PickedAt.Time.String()
	}

	// set the startedAt time if not null
	if task.StartedAt.Status == pgtype.Present {
		response.StartedAt = task.StartedAt.Time.String()
	}

	// set the completedAt time if not null
	if task.CompletedAt.Status == pgtype.Present {
		response.CompletedAt = task.CompletedAt.Time.String()
	}

	// set the failedAt time if not null
	if task.FailedAt.Status == pgtype.Present {
		response.FailedAt = task.FailedAt.Time.String()
	}

	// convert the response to JSON
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal response. Error: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	// set the content type and write the response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)
}

// insertTaskIntoDB inserts a new task into the database and returns the task ID
func (s *SchedulerServer) insertTaskIntoDB(ctx context.Context, task Task) (string, error) {
	// query to insert the task into the database
	query := `
		INSERT INTO tasks (command, schedule_at)
		VALUES ($1, $2)
		RETURNING id
		`
	var insertedID string
	err := s.dbPool.QueryRow(ctx, query, task.Command, task.ScheduleAt.Time).Scan(&insertedID)
	if err != nil {
		return "", fmt.Errorf("failed to insert task into database: %w", err)
	}
	return insertedID, nil
}

func (s *SchedulerServer) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	return s.Stop()
}

func (s *SchedulerServer) Stop() error {
	s.dbPool.Close()
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}
	log.Printf("Scheduler server and database pool stopped")
	return nil
}
