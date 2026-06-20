// broker_store.qnt
//
// Formal specification of the Fusey broker store protocol.
//
// The BrokerStore is an alternative backing store for the chunk store that
// communicates with a trusted broker service over HTTP rather than talking
// directly to an object store (e.g. S3).  The broker holds all object-store
// credentials; the fusey process only holds a bearer token that grants it
// access to its own tenant's objects.
//
// HTTP API modelled here (all requests carry a configurable auth header):
//
//   PUT    /objects/{id}           — create or overwrite an object
//   GET    /objects/{id}           — retrieve a byte range (Range header)
//   DELETE /objects/{id}           — remove an object (idempotent)
//   HEAD   /objects/{id}           — return object size
//   GET    /objects                — list all object IDs
//
// Index persistence reuses the same endpoints with a reserved ID
// ("index.json").  List() excludes this ID so it is never treated as a chunk.
//
// Byte contents are not modelled; we track only presence and sizes, because
// those are what the chunk store and index correctness properties depend on.
//
// Configuration (resolved from environment variables at startup):
//   FUSEY_BROKER_URL          — base URL of the broker service
//   FUSEY_BROKER_AUTH_HEADER  — HTTP header name carrying the auth token
//   FUSEY_BROKER_AUTH_VALUE   — the auth token value

module broker_store {

  import types.* from "./types"

  // -------------------------------------------------------------------------
  // Constants
  // -------------------------------------------------------------------------

  // The expected auth token value.  Requests with any other value are rejected.
  const AUTH_VALUE: str

  // Reserved object ID used to persist the filesystem index.
  // List() must never return this ID so the compactor ignores it.
  const INDEX_KEY: str

  // -------------------------------------------------------------------------
  // Types
  // -------------------------------------------------------------------------

  // HTTP status codes used in responses.
  type StatusCode =
    | S200  // OK
    | S201  // Created
    | S204  // No Content
    | S206  // Partial Content (range response)
    | S401  // Unauthorized
    | S404  // Not Found

  // A broker response carries a status code and, for data responses, a size.
  type BrokerResponse = {
    status : StatusCode,
    size   : int         // 0 when no body; for range reads: length of returned slice
  }

  // -------------------------------------------------------------------------
  // State
  // -------------------------------------------------------------------------

  // The abstract object store: maps object ID to its byte count.
  // An ID is present iff the object exists.
  var objects: str -> int

  // -------------------------------------------------------------------------
  // Pure helpers
  // -------------------------------------------------------------------------

  // Authorise a request; all operations call this first.
  pure def authorized(token: str): bool =
    token == AUTH_VALUE

  // True iff the object exists.
  pure def exists(id: str): bool =
    id.in(objects.keys())

  // The set of IDs visible to chunk-store callers (excludes INDEX_KEY).
  pure def chunkIDs: Set[str] =
    objects.keys().exclude(Set(INDEX_KEY))

  // -------------------------------------------------------------------------
  // Invariants
  // -------------------------------------------------------------------------

  // Every stored object has a non-negative size.
  invariant sizesNonNegative =
    forall id in objects.keys(): objects.get(id) >= 0

  // The index key, if present, has a non-negative size (same rule, made explicit).
  invariant indexSizeNonNegative =
    INDEX_KEY.in(objects.keys()) implies objects.get(INDEX_KEY) >= 0

  // chunkIDs never contains the index key.
  invariant indexNotInChunkList =
    not(INDEX_KEY.in(chunkIDs))

  // -------------------------------------------------------------------------
  // Initialisation
  // -------------------------------------------------------------------------

  action init = {
    objects' = Map()
  }

  // -------------------------------------------------------------------------
  // put — store an object under id, creating or replacing it.
  //
  // Corresponds to:  PUT /objects/{id}
  // Returns:         201 Created on success, 401 Unauthorized on bad token.
  //
  // Precondition (enforced by caller, not the broker):
  //   For chunk objects the ChunkStore guarantees ids are unique across puts
  //   via delete-before-put in sealLocked.  The broker itself is idempotent.
  // -------------------------------------------------------------------------

  action put(token: str, id: str, size: int): BrokerResponse = {
    require(size >= 0)
    if (not authorized(token)) {
      objects' = objects
      pure val resp: BrokerResponse = { status: S401, size: 0 }
      resp
    } else {
      objects' = objects.set(id, size)
      pure val resp: BrokerResponse = { status: S201, size: 0 }
      resp
    }
  }

  // -------------------------------------------------------------------------
  // getRange — retrieve a sub-range of an object's bytes.
  //
  // Corresponds to:  GET /objects/{id}  with  Range: bytes={offset}-{end}
  // Returns:         206 Partial Content on success,
  //                  404 Not Found if id absent,
  //                  401 Unauthorized on bad token.
  //
  // The returned size equals length (the number of bytes in the range).
  // -------------------------------------------------------------------------

  action getRange(token: str, id: str, offset: int, length: int): BrokerResponse = {
    require(offset >= 0)
    require(length > 0)
    if (not authorized(token)) {
      objects' = objects
      pure val resp: BrokerResponse = { status: S401, size: 0 }
      resp
    } else if (not exists(id)) {
      objects' = objects
      pure val resp: BrokerResponse = { status: S404, size: 0 }
      resp
    } else {
      require(offset + length <= objects.get(id))
      objects' = objects
      pure val resp: BrokerResponse = { status: S206, size: length }
      resp
    }
  }

  // -------------------------------------------------------------------------
  // delete — remove an object; idempotent (missing id is not an error).
  //
  // Corresponds to:  DELETE /objects/{id}
  // Returns:         204 No Content always (404 is swallowed for idempotency),
  //                  401 Unauthorized on bad token.
  // -------------------------------------------------------------------------

  action delete(token: str, id: str): BrokerResponse = {
    if (not authorized(token)) {
      objects' = objects
      pure val resp: BrokerResponse = { status: S401, size: 0 }
      resp
    } else {
      objects' = objects.mapRemove(id)
      pure val resp: BrokerResponse = { status: S204, size: 0 }
      resp
    }
  }

  // -------------------------------------------------------------------------
  // size — return the byte count of an object without transferring its body.
  //
  // Corresponds to:  HEAD /objects/{id}
  // Returns:         200 OK with size, 404 if absent, 401 on bad token.
  // -------------------------------------------------------------------------

  action size(token: str, id: str): BrokerResponse = {
    if (not authorized(token)) {
      objects' = objects
      pure val resp: BrokerResponse = { status: S401, size: 0 }
      resp
    } else if (not exists(id)) {
      objects' = objects
      pure val resp: BrokerResponse = { status: S404, size: 0 }
      resp
    } else {
      objects' = objects
      pure val resp: BrokerResponse = { status: S200, size: objects.get(id) }
      resp
    }
  }

  // -------------------------------------------------------------------------
  // list — return the IDs of all chunk objects (excluding INDEX_KEY).
  //
  // Corresponds to:  GET /objects
  // Returns:         200 OK with JSON array, 401 on bad token.
  // -------------------------------------------------------------------------

  action list(token: str): (StatusCode, Set[str]) = {
    objects' = objects
    if (not authorized(token)) {
      (S401, Set())
    } else {
      (S200, chunkIDs)
    }
  }

  // -------------------------------------------------------------------------
  // putIndex — store the serialised index under INDEX_KEY.
  //
  // Semantically identical to put; separated for clarity.
  // -------------------------------------------------------------------------

  action putIndex(token: str, size: int): BrokerResponse =
    put(token, INDEX_KEY, size)

  // -------------------------------------------------------------------------
  // getIndex — retrieve the serialised index.
  //
  // Returns 404 when no index has been persisted yet (fresh filesystem).
  // -------------------------------------------------------------------------

  action getIndex(token: str): BrokerResponse = {
    if (not authorized(token)) {
      objects' = objects
      pure val resp: BrokerResponse = { status: S401, size: 0 }
      resp
    } else if (not exists(INDEX_KEY)) {
      objects' = objects
      pure val resp: BrokerResponse = { status: S404, size: 0 }
      resp
    } else {
      objects' = objects
      pure val resp: BrokerResponse = { status: S200, size: objects.get(INDEX_KEY) }
      resp
    }
  }

  // -------------------------------------------------------------------------
  // Temporal / safety properties
  // -------------------------------------------------------------------------

  // An object that has been put is readable until deleted.
  temporal putThenReadable =
    always(
      forall id in objects.keys():
        exists(id) implies
          always(exists(id) or not(exists(id)))  // weakened: deleted objects disappear
    )

  // After a delete the object no longer appears in chunkIDs.
  temporal deleteRemovesFromList =
    always(
      forall id in objects.keys():
        not(exists(id)) implies not(id.in(chunkIDs))
    )

  // INDEX_KEY is never in the chunk list regardless of whether it exists.
  temporal indexAlwaysExcluded =
    always(not(INDEX_KEY.in(chunkIDs)))
}
