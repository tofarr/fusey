"""
Fusey broker — reference implementation.

A trusted intermediary between fusey nodes and an object store. In this
reference implementation the object store is a local directory; production
deployments would replace the storage backend with S3, GCS, Azure Blob, etc.

Fusey nodes never hold object-store credentials. They authenticate to this
broker with a bearer token (configurable header name and value) and receive
access only to their own tenant's objects.

Environment variables
---------------------
FUSEY_STORE_DIR          Directory used to persist objects.
                         Default: ./broker_store
FUSEY_BROKER_AUTH_HEADER HTTP header name carrying the auth token.
                         Default: X-SESSION-API-KEY
FUSEY_BROKER_AUTH_VALUE  Expected auth token value. Required.
                         Default: changeThis   (change for production)

HTTP API
--------
All requests must carry the configured auth header.

  PUT    /objects/{id}   Store an object. Body is raw bytes. Returns 201.
  GET    /objects/{id}   Retrieve an object. Honours the Range header (206).
  DELETE /objects/{id}   Remove an object (idempotent). Returns 204.
  HEAD   /objects/{id}   Return Content-Length only. Returns 200.
  GET    /objects        Return a JSON array of all object IDs.

Object IDs must contain only alphanumerics, hyphens, underscores, and dots.
IDs containing path separators are rejected with 400.

Running
-------
  pip install -r requirements.txt
  FUSEY_BROKER_AUTH_VALUE=secret uvicorn broker.main:app --reload
"""

import os
import re
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STORE_DIR = Path(os.getenv("FUSEY_STORE_DIR", "./broker_store"))
AUTH_HEADER = os.getenv("FUSEY_BROKER_AUTH_HEADER", "X-SESSION-API-KEY")
AUTH_VALUE = os.getenv("FUSEY_BROKER_AUTH_VALUE", "changeThis")

# Allowlist pattern for object IDs. Prevents path traversal.
_SAFE_ID = re.compile(r"^[A-Za-z0-9._-]+$")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Fusey broker", version="1.0.0")


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

def require_auth(request: Request) -> None:
    """Validate the auth header on every request."""
    if request.headers.get(AUTH_HEADER) != AUTH_VALUE:
        raise HTTPException(status_code=401, detail="Unauthorized")


def validate_id(object_id: str) -> str:
    """Reject object IDs that contain unsafe characters."""
    if not _SAFE_ID.match(object_id):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid object ID {object_id!r}: only [A-Za-z0-9._-] allowed",
        )
    return object_id


def object_path(object_id: str) -> Path:
    """Return the filesystem path for an object, creating the store dir if needed."""
    STORE_DIR.mkdir(parents=True, exist_ok=True)
    return STORE_DIR / object_id


Auth = Annotated[None, Depends(require_auth)]
SafeID = Annotated[str, Depends(validate_id)]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.put("/objects/{object_id}", status_code=201, dependencies=[Depends(require_auth)])
async def put_object(object_id: SafeID, request: Request) -> Response:
    """Store an object. Creates or replaces."""
    data = await request.body()
    object_path(object_id).write_bytes(data)
    return Response(status_code=201)


@app.get("/objects/{object_id}", dependencies=[Depends(require_auth)])
async def get_object(object_id: SafeID, request: Request) -> Response:
    """Retrieve an object, honouring the Range header for partial content."""
    path = object_path(object_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Not found")

    data = path.read_bytes()
    range_header = request.headers.get("range", "")

    if not range_header:
        return Response(
            content=data,
            status_code=200,
            media_type="application/octet-stream",
            headers={"Content-Length": str(len(data))},
        )

    # Parse "bytes=start-end"
    try:
        range_val = range_header.removeprefix("bytes=")
        start_str, end_str = range_val.split("-", 1)
        start, end = int(start_str), int(end_str)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=416, detail="Invalid Range header")

    if start < 0 or end < start or end >= len(data):
        raise HTTPException(
            status_code=416,
            detail=f"Range {start}-{end} out of bounds for object of size {len(data)}",
        )

    slice_ = data[start : end + 1]
    return Response(
        content=slice_,
        status_code=206,
        media_type="application/octet-stream",
        headers={"Content-Length": str(len(slice_))},
    )


@app.delete("/objects/{object_id}", status_code=204, dependencies=[Depends(require_auth)])
async def delete_object(object_id: SafeID) -> Response:
    """Remove an object. Idempotent: missing objects are not an error."""
    path = object_path(object_id)
    if path.exists():
        path.unlink()
    return Response(status_code=204)


@app.head("/objects/{object_id}", dependencies=[Depends(require_auth)])
async def head_object(object_id: SafeID) -> Response:
    """Return object metadata (Content-Length) without the body."""
    path = object_path(object_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return Response(
        status_code=200,
        headers={"Content-Length": str(path.stat().st_size)},
    )


@app.get("/objects", dependencies=[Depends(require_auth)])
async def list_objects() -> JSONResponse:
    """Return a JSON array of all stored object IDs."""
    if not STORE_DIR.exists():
        return JSONResponse([])
    ids = [f.name for f in STORE_DIR.iterdir() if f.is_file()]
    return JSONResponse(ids)
