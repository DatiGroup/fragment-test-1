import os
import json
import asyncio
from datetime import datetime, timezone
from fastapi import FastAPI, Depends, WebSocket
from sqlalchemy.orm import Session
from dt_fragment_auth import FragmentAuth
from dt_fragment_auth.fastapi import accept_websocket, protect_http

# import local modules directly (the app is copied into /app in the container build)
import database, crud, schemas, models

# Every HTTP route except /healthcheck admits only members of this fragment's
# digital twin (the widget sends the user's token), other fragments of the twin
# (the twin's Keycloak client) and this fragment's own worker (internal token).
# Deny by default: routes added later are protected too. Settings come from
# the platform; see dt-fragment-auth's README.
auth = FragmentAuth.from_env()
app = FastAPI(title="Counter API", dependencies=[Depends(protect_http(auth))])

# For development convenience only — migrations should be used in production
models.Base.metadata.create_all(bind=database.engine)

COUNTER_WS_INTERVAL = int(os.getenv("COUNTER_WS_INTERVAL", "2"))


@app.get("/healthcheck")
def healthcheck():
    return {"status": "ok"}


@app.get("/counter", response_model=schemas.CounterOut)
def get_counter(db: Session = Depends(database.get_db)):
    counter = crud.get_counter(db)
    if not counter:
        return {"value": 0}
    return {"value": counter.value}


@app.post("/counter", response_model=schemas.CounterOut)
def set_counter(payload: schemas.CounterIn, db: Session = Depends(database.get_db)):
    counter = crud.create_or_update_counter(db, payload.value)
    return {"value": counter.value}


@app.websocket("/ws/counter")
async def ws_counter(websocket: WebSocket):
    # Nothing is sent until the client's {type: authenticate, token} is valid;
    # the client renews the token on the same socket before it expires.
    session = await accept_websocket(auth, websocket)
    if session is None:
        return
    try:
        while session.active:
            db = database.SessionLocal()
            try:
                counter = crud.get_counter(db)
                value = counter.value if counter else 0
            finally:
                db.close()
            await session.send_json({
                "value": value,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            await asyncio.sleep(COUNTER_WS_INTERVAL)
    finally:
        await session.close()
