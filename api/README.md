# Counter API (FastAPI)

Simple FastAPI app that stores a single counter value in PostgreSQL.

Endpoints:
- GET /healthcheck -> {"status":"ok"}
- GET /counter -> {"value": <int>}
- POST /counter -> {"value": <int>}    (body: {"value": <int>})

Setup

1. Create a virtualenv and install dependencies:

   python -m venv .venv
   .\.venv\Scripts\activate
   pip install -r requirements.txt

2. Copy `.env.example` to `.env` and edit `DATABASE_URL`.

3. Create the database in PostgreSQL and run migrations:

   alembic upgrade head

4. Run the app:

   uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

Notes

- The project uses SQLAlchemy and Alembic. Alembic is configured to read `DATABASE_URL` from the environment.
- The `counter` table stores one row with `id=1`. The app will treat absence of the row as 0 until you POST a value.

---

## 🐳 Docker

Build the image (run from inside the `api` folder):

    docker build -t counter-api .

Run the container (using your `.env`):

    docker run --env-file .env -p 8000:8000 counter-api

Notes:
- The entrypoint runs `alembic upgrade head` before starting the server (best-effort).
- If you prefer compose, I can add a `docker-compose.yml` that runs Postgres + this app for local testing.
