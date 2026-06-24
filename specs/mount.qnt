// mount.qnt
//
// Specification of the fusey mount process lifecycle and ready-signal protocol.
//
// Problem being modelled:
//   fusey is launched as a background process by an external orchestrator
//   (the runtime-api).  The orchestrator must not send /api/init to the
//   agent-server until the FUSE filesystem is fully accessible.  Previously
//   the orchestrator used a fixed sleep, which is inherently racy because
//   fusey's startup time varies: it must (1) load config, (2) connect to the
//   broker/S3, (3) load the index, and (4) call mount(2) before the
//   filesystem is usable.
//
// Solution being modelled:
//   fusey writes a sentinel file ({FUSEY_CACHE_DIR}/ready) after mount(2)
//   returns successfully.  The orchestrator polls for this file; once it
//   appears the filesystem is guaranteed accessible.  The sentinel is removed
//   at process start (to clear stale files from prior runs) and again just
//   before unmount.
//
// Correspondence to Go implementation (cmd/fusey/main.go):
//   Initializing  → process started, before gofs.Mount returns
//   Ready         → after gofs.Mount returns; sentinel file written
//   Stopped       → after SIGTERM/SIGINT received; sentinel removed; unmount complete

module mount {

  // --------------------------------------------------------------------------
  // Types
  // --------------------------------------------------------------------------

  type MountState =
    | Initializing  // startup: loading config, index, calling mount(2)
    | Ready         // FUSE mount established; filesystem accessible
    | Stopped       // unmount complete; process will exit

  // --------------------------------------------------------------------------
  // State
  // --------------------------------------------------------------------------

  // Lifecycle position of the fusey process.
  var mountState: MountState

  // Whether the ready sentinel file exists on disk.
  // In the implementation this is {FUSEY_CACHE_DIR}/ready.
  var sentinelExists: bool

  // --------------------------------------------------------------------------
  // Initialisation
  // --------------------------------------------------------------------------

  // At process start the sentinel from any previous run is removed (the
  // implementation calls os.Remove before doing any other work).
  action init = {
    mountState'    = Initializing
    sentinelExists' = false
  }

  // --------------------------------------------------------------------------
  // Actions
  // --------------------------------------------------------------------------

  // becomeReady: fusey has finished loading the index and mount(2) has
  // returned successfully.  The FUSE filesystem is now accessible to other
  // processes.  The sentinel file is written atomically before this action
  // completes so that any poller that observes the sentinel is guaranteed
  // to see a working filesystem.
  //
  // Corresponds to: the os.WriteFile(readyPath, ...) call in runMount,
  // executed immediately after gofs.Mount returns without error.
  action becomeReady = {
    require(mountState == Initializing)
    mountState'    = Ready
    sentinelExists' = true
  }

  // beginUnmount: a shutdown signal (SIGTERM or SIGINT) is received.
  // The sentinel is removed before unmount begins so that any poller
  // that is still running will not see the sentinel after the filesystem
  // is torn down.
  //
  // Corresponds to: os.Remove(readyPath) called in the signal handler,
  // before server.Unmount().
  action beginUnmount = {
    require(mountState == Ready)
    mountState'    = Stopped
    sentinelExists' = false
  }

  // --------------------------------------------------------------------------
  // Invariants
  // --------------------------------------------------------------------------

  // Core invariant: the sentinel file exists if and only if the mount is Ready.
  // This is the safety property the orchestrator's poll relies on.
  invariant sentinelIffReady =
    sentinelExists iff mountState == Ready

  // The filesystem is never both stopped and ready.
  invariant notStoppedAndReady =
    not(mountState == Stopped and sentinelExists)

  // --------------------------------------------------------------------------
  // Temporal properties
  // --------------------------------------------------------------------------

  // Once startup begins, Ready is eventually reached (liveness: fusey always
  // finishes mounting under fair scheduling).
  temporal eventualReady =
    always(mountState == Initializing implies eventually(mountState == Ready))

  // Once Ready, the process eventually stops (liveness: fusey always
  // terminates when a signal arrives).
  temporal eventualStop =
    always(mountState == Ready implies eventually(mountState == Stopped))

  // The sentinel is never present in a Stopped or Initializing state.
  temporal sentinelOnlyWhileReady =
    always(sentinelExists implies mountState == Ready)

}
