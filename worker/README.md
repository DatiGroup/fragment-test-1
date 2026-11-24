# Worker

Simple Python worker that increments the Counter API value by 1 every INTERVAL seconds.

Setup

1. Create a virtualenv and install deps:

   python -m venv .venv
   .\.venv\Scripts\activate
   pip install -r requirements.txt

2. Copy `.env.example` to `.env` and edit `API_URL` if needed.

Run

   python main.py

Docker

Build the worker image (from repo root):

    docker build -t counter-worker ./worker

Run the worker (will use `worker/.env`):

    docker run --env-file worker/.env counter-worker

You can also use the root `docker-compose.yml` to run both services (API + worker) once Postgres is added and configured.

Notes

- The worker calls `GET /counter` then `POST /counter` with `value+1`.
- It's intentionally simple; if you want, I can add a Dockerfile and a `docker-compose` service for the worker.
