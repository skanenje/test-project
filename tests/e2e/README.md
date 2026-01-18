# End-to-End Tests

## Purpose

The `e2e` test package contains end-to-end tests for the entire database system, including the web server and HTTP API. These tests verify that:
- The web server starts and accepts connections
- HTTP requests are properly routed and handled
- Database operations work through the REST API
- Response formats are correct
- Error handling works end-to-end

## Test Files

### web_test.go
HTTP API tests:
- Server startup and initialization
- GET /tasks endpoint
- POST /tasks endpoint (create)
- PUT /tasks endpoint (update)
- DELETE /tasks endpoint
- Response status codes and formatting
- Error responses
- Concurrent request handling

## Running Tests

```bash
# Run all e2e tests
go test ./tests/e2e -v

# Run specific test
go test ./tests/e2e -run TestWebServer -v

# Run with coverage
go test ./tests/e2e -v -cover
```

## Test Structure

E2E tests typically:
1. Start a database instance
2. Initialize the web server
3. Make HTTP requests to the server
4. Verify responses
5. Clean up

Example:

```go
func TestWebGetTasks(t *testing.T) {
    // Setup
    tmpDir := t.TempDir()
    db, _ := database.New(tmpDir)
    app := web.New(db)
    app.Initialize()

    // Start HTTP server
    mux := http.NewServeMux()
    mux.HandleFunc("/tasks", app.Handle)
    server := httptest.NewServer(mux)
    defer server.Close()

    // Test
    resp, err := http.Get(server.URL + "/tasks")
    if err != nil {
        t.Fatal(err)
    }
    defer resp.Body.Close()

    // Assert
    if resp.StatusCode != http.StatusOK {
        t.Errorf("got status %d, want %d", resp.StatusCode, http.StatusOK)
    }
}
```

## Using httptest

The standard library's `httptest` package simplifies testing HTTP handlers:

```go
import "net/http/httptest"

// Create a test server
server := httptest.NewServer(mux)
defer server.Close()

// Make requests
resp, err := http.Get(server.URL + "/tasks")
resp, err := http.Post(server.URL + "/tasks", "application/json", body)
```

## Assertions

Common HTTP test assertions:

```go
// Check status code
if resp.StatusCode != http.StatusOK {
    t.Errorf("status: got %d, want %d", resp.StatusCode, http.StatusOK)
}

// Check Content-Type
if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
    t.Errorf("content-type: got %s, want application/json", ct)
}

// Parse JSON response
var data map[string]interface{}
json.NewDecoder(resp.Body).Decode(&data)
```

## Integration Points

- **Web Package**: Tests the REST API
- **Database Package**: Verifies data operations through API
- **HTTP Handler Testing**: Uses httptest for server testing
