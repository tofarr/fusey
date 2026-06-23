"""
Fusey broker — reference implementation.

A trusted intermediary between fusey pods and an object store.  In this
reference implementation the "object store" is a local directory; production
deployments replace the storage backend with real S3, GCS, or Azure Blob and
generate genuine presigned URLs.

Fusey pods never hold object-store credentials.  They authenticate to this
broker with a bearer token, receive a short-lived presigned URL, then transfer
bytes directly to/from the object store — keeping the broker out of the data
path entirely.

Protocol
--------
Write flow (fusey → broker → S3):
  1. GET  /objects/{id}/upload-url    (auth header required)
         → {"url": "<presigned PUT URL>"}
  2. PUT  bytes directly to the presigned URL (no auth header)

Read flow (fusey → broker → S3):
  1. GET  /objects/{id}/download-url  (auth header required)
         → {"url": "<presigned GET URL>"}
  2. GET  presigned URL with Range header (no auth header)

Metadata / control operations (auth header required):
  DELETE /objects/{id}   — remove an object (idempotent)
  HEAD   /objects/{id}   — return Content-Length only
  GET    /objects        — return a JSON array of all object IDs

Reference-implementation note
------------------------------
This server has no S3 backend, so presigned URLs are self-referential:
they point to /raw/{id} on this same server, which serves bytes without
an auth header (mimicking how S3 embeds credentials in the URL itself).

Environment variables
---------------------
FUSEY_STORE_DIR          Directory used to persist objects.
                         Default: ./broker_store
FUSEY_BROKER_AUTH_HEADER HTTP header name carrying the auth token.
                         Default: X-SESSION-API-KEY
FUSEY_BROKER_AUTH_VALUE  Expected auth token value.
                         Default: changeThis  (change for production)
FUSEY_BROKER_BASE_URL    Public base URL of this server, used to build
                         self-referential presigned URLs.
                         Default: http://localhost:8000

Object IDs must contain only alphanumerics, hyphens, underscores, and dots.

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
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STORE_DIR = Path(os.getenv("FUSEY_STORE_DIR", "./broker_store"))
AUTH_HEADER = os.getenv("FUSEY_BROKER_AUTH_HEADER", "X-SESSION-API-KEY")
AUTH_VALUE = os.getenv("FUSEY_BROKER_AUTH_VALUE", "changeThis")
# Used to build self-referential presigned URLs. Override with the public URL
# of this server when running behind a proxy or in a container.
BASE_URL = os.getenv("FUSEY_BROKER_BASE_URL", "http://localhost:8000").rstrip("/")

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
# Routes — presigned URL endpoints (auth required, no byte transfer)
# ---------------------------------------------------------------------------

@app.get("/objects/{object_id}/upload-url", dependencies=[Depends(require_auth)])
async def upload_url(object_id: SafeID) -> JSONResponse:
    """Return a presigned PUT URL for direct upload to the object store.

    In production this would call S3/GCS to generate a short-lived signed URL.
    Here we return a self-referential URL pointing to /raw/{id}, which accepts
    bytes without an auth header — mimicking S3 presigned URL behaviour.
    """
    return JSONResponse({"url": f"{BASE_URL}/raw/{object_id}"})


@app.get("/objects/{object_id}/download-url", dependencies=[Depends(require_auth)])
async def download_url(object_id: SafeID) -> JSONResponse:
    """Return a presigned GET URL for direct download from the object store.

    Returns 404 if the object does not exist, so callers learn of missing
    objects at presign time rather than at download time.
    """
    if not object_path(object_id).exists():
        raise HTTPException(status_code=404, detail="Not found")
    return JSONResponse({"url": f"{BASE_URL}/raw/{object_id}"})


# ---------------------------------------------------------------------------
# Routes — metadata / control (auth required, no byte transfer)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Routes — raw object store (no auth: credentials embedded in URL by design)
#
# These endpoints are the target of self-referential presigned URLs returned
# by /upload-url and /download-url.  They simulate the role of S3: they
# accept and serve bytes without an auth header because the URL itself is the
# credential (time-limited in a real deployment; unlimited here for simplicity).
# ---------------------------------------------------------------------------

@app.put("/raw/{object_id}", status_code=201)
async def raw_put(object_id: SafeID, request: Request) -> Response:
    """Accept a direct object upload (simulates S3 presigned PUT target)."""
    data = await request.body()
    object_path(object_id).write_bytes(data)
    return Response(status_code=201)


@app.get("/raw/{object_id}")
async def raw_get(object_id: SafeID, request: Request) -> Response:
    """Serve an object with Range support (simulates S3 presigned GET target)."""
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
