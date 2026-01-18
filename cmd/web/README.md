# Web Package

## Purpose

The `web` package provides a REST API web interface for database interaction. It implements a task management API (`TaskApp`) demonstrating how to build web applications on the RDBMS.

## REST API Pattern

- `GET /tasks` - List all tasks
- `GET /tasks/:id` - Get specific task
- `POST /tasks` - Create new task
- `PUT /tasks/:id` - Update task
- `DELETE /tasks/:id` - Delete task

## Request/Response Format

JSON requests and responses:

```json
{
    "id": 1,
    "title": "Buy groceries",
    "completed": false
}
```

## Key Types

```go
type TaskApp struct {
    db *database.Database
}

type Task struct {
    ID        int    `json:"id"`
    Title     string `json:"title"`
    Completed bool   `json:"completed"`
}
```

## Main Functions

- `New(db *database.Database) *TaskApp` - Create app
- `(app *TaskApp) Initialize() error` - Create tasks table
- `(app *TaskApp) Handle(w http.ResponseWriter, r *http.Request)` - Main handler
- `(app *TaskApp) handleGetTasks(w, r)` - GET /tasks
- `(app *TaskApp) handleCreateTask(w, r)` - POST /tasks
- `(app *TaskApp) handleUpdateTask(w, r)` - PUT /tasks
- `(app *TaskApp) handleDeleteTask(w, r)` - DELETE /tasks

## Usage Example

```go
db, _ := database.New("/path/to/data")
app := web.New(db)
app.Initialize()

http.HandleFunc("/tasks", app.Handle)
log.Fatal(http.ListenAndServe(":8080", nil))
```

## API Examples

```bash
# Get all tasks
curl http://localhost:8080/tasks

# Create task
curl -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{"title": "Buy groceries", "completed": false}'

# Update task
curl -X PUT http://localhost:8080/tasks/1 \
  -H "Content-Type: application/json" \
  -d '{"title": "Buy milk", "completed": true}'

# Delete task
curl -X DELETE http://localhost:8080/tasks/1
```

## Table Schema

| Column | Type | Key |
|--------|------|-----|
| id | INT | PRIMARY |
| title | TEXT | |
| completed | BOOL | |

## HTTP Status Codes

- `200 OK` - Success
- `201 Created` - Resource created
- `400 Bad Request` - Invalid request
- `404 Not Found` - Resource missing
- `500 Internal Server Error` - Database error

## Integration Points

- **Database Package**: All data operations
- **Schema Package**: Table definitions
- **Main Package**: Server initialization
