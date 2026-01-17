// Package web provides a REST API web server for database operations.
//
// The web package implements a simple HTTP server that exposes database operations
// through REST endpoints. It provides a task management API as a demonstration of
// how to use the database package in a web application context.
//
// Key Features:
//   - RESTful API endpoints for CRUD operations
//   - JSON request/response format
//   - Task management example application
//   - HTTP method routing (GET, POST, PUT, DELETE)
//
// API Endpoints:
//   - GET /tasks: Retrieve all tasks
//   - POST /tasks: Create a new task
//   - PUT /tasks?id=<id>: Update an existing task
//   - DELETE /tasks?id=<id>: Delete a task
//
// Key Responsibilities:
//   - Initializing the tasks table schema
//   - Handling HTTP requests and routing to appropriate handlers
//   - Converting between JSON and database row formats
//   - Managing HTTP response codes and error handling
//   - Providing a simple REST API interface
//
// Usage Example:
//
//	db, _ := database.New("./data")
//	app := web.New(db)
//	app.Initialize()
//
//	http.HandleFunc("/tasks", app.Handle)
//	http.ListenAndServe(":8080", nil)
//
// The web package demonstrates how to use the database package in a web application.
// It shows best practices for:
//   - Initializing database schemas
//   - Handling HTTP requests
//   - Converting between JSON and database formats
//   - Error handling and HTTP status codes
//
// This package can serve as a template for building more complex web applications
// on top of the database system.
package web
