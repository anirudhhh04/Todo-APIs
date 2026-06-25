# TaskMake

TaskMake is a full-stack Todo Management application built from scratch using a custom C++ HTTP server and a React frontend.
This project implements core backend functionalities manually, including HTTP request parsing, routing, middleware handling and thread pooling.



## Tech Stack

### Backend

* C++
* POSIX Sockets
* Multithreading

### Frontend

* React.js
* CSS



## API Endpoints

| Method | Endpoint          | Description             |
| ------ | ----------------- | ----------------------- |
| GET    | `/`               | Health check endpoint   |
| POST   | `/create`         | Create a new todo       |
| GET    | `/todos`          | Retrieve all todos      |
| GET    | `/get?id={id}`    | Retrieve a todo by ID   |
| PUT    | `/update`         | Update an existing todo |
| DELETE | `/delete?id={id}` | Delete a todo by ID     |

