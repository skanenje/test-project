package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"rdbms/database"
	"rdbms/parser"
	"rdbms/schema"
	"rdbms/storage"
)

// TaskApp is a simple REST API for tasks
type TaskApp struct {
	db *database.Database
}

func New(db *database.Database) *TaskApp {
	return &TaskApp{db: db}
}

func (app *TaskApp) Initialize() error {
	if _, err := app.db.GetTable("tasks"); err != nil {
		cols := []schema.Column{
			{Name: "id", Type: schema.TypeInt, PrimaryKey: true},
			{Name: "title", Type: schema.TypeText},
			{Name: "completed", Type: schema.TypeBool},
		}
		return app.db.CreateTable("tasks", cols)
	}
	return nil
}

func (app *TaskApp) Handle(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		app.handleGetTasks(w, r)
	case http.MethodPost:
		app.handleCreateTask(w, r)
	case http.MethodPut:
		app.handleUpdateTask(w, r)
	case http.MethodDelete:
		app.handleDeleteTask(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (app *TaskApp) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := app.db.Select("tasks", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(rows)
}

func (app *TaskApp) handleCreateTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var task storage.Row
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if _, ok := task["completed"]; !ok {
		task["completed"] = false
	}

	rowID, err := app.db.Insert("tasks", task)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := map[string]interface{}{"message": "Task created", "row_id": rowID, "task": task}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func (app *TaskApp) handleUpdateTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		http.Error(w, "Missing 'id' parameter", http.StatusBadRequest)
		return
	}

	id, err := strconv.ParseFloat(idStr, 64)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	var updates storage.Row
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	where := &parser.WhereClause{Column: "id", Value: id}
	existing, err := app.db.Select("tasks", where)
	if err != nil || len(existing) == 0 {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	// merge
	task := existing[0]
	for k, v := range updates {
		task[k] = v
	}

	// delete old and insert new (database.Update supports single-column updates; use delete+insert for full-row)
	if _, err := app.db.Delete("tasks", where); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	newID, err := app.db.Insert("tasks", task)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{"message": "Task updated", "task": task, "new_id": newID}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (app *TaskApp) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		http.Error(w, "Missing 'id' parameter", http.StatusBadRequest)
		return
	}

	id, err := strconv.ParseFloat(idStr, 64)
	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}

	where := &parser.WhereClause{Column: "id", Value: id}
	count, err := app.db.Delete("tasks", where)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if count == 0 {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	resp := map[string]string{"message": "Task deleted"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// RunServer starts the HTTP server with the task API endpoints.
// It initializes the TaskApp, sets up route handlers, and listens on the specified port.
func RunServer(db *database.Database, port string) error {
	app := New(db)
	if err := app.Initialize(); err != nil {
		return fmt.Errorf("error initializing task app: %w", err)
	}

	http.HandleFunc("/tasks", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			app.handleGetTasks(w, r)
		case http.MethodPost:
			app.handleCreateTask(w, r)
		case http.MethodPut:
			app.handleUpdateTask(w, r)
		case http.MethodDelete:
			app.handleDeleteTask(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	fmt.Printf("🚀 Task API server running on http://localhost:%s\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		return fmt.Errorf("error starting server: %w", err)
	}

	return nil
}
