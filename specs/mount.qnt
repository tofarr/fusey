// mount.qnt
//
// Specification of the fusey daemon-based mount lifecycle.
//
// Design overview:
//   `fusey mount <mountpoint>` forks a background daemon
//   (`fusey daemon <id> <mountpoint>`) and exits as soon as the mount is
//   operational.  Multiple mounts are supported concurrently; each daemon
//   gets an isolated subdirectory under FUSEY_CACHE_DIR named by a random
//   daemon ID, and writes a JSON PID file there so that `fusey unmount`
//   can locate and terminate the right process.
//
// Ready-signal protocol:
//   The parent creates an OS pipe before forking.  The write end (fd 3) is
//   inherited by the daemon.  Once gofs.Mount returns the daemon writes
//   "ready\n" to fd 3 and closes it.  The parent reads the pipe; when it
//   receives "ready\n" it exits 0.  If the pipe closes without that string
//   the daemon failed and the parent exits non-zero.
//
// Shutdown protocol:
//   On SIGTERM or SIGINT the daemon transitions through three phases:
//     DaemonStopping  — signal received; background context cancelled;
//                       FUSE server unmounting (no new kernel requests).
//     DaemonFlushing  — FUSE unmounted; final index flush running under a
//                       fresh context with a 60-second budget so the remote
//                       write is not cancelled along with the background ctx.
//     DaemonStopped   — flush complete; PID file removed; process exited.
//   The two-phase split (DaemonFlushing vs DaemonStopped) is the fix for the
//   bug where cancel() was called before persistFn(), causing every S3/broker
//   call in the final flush to return context.Canceled immediately.
//
// Correspondence to Go implementation (cmd/fusey/main.go):
//   ParentForking     → runMount before cmd.Start
//   ParentWaiting     → runMount blocking on io.ReadAll(r)
//   ParentDone        → runMount after fmt.Printf; exits 0
//   ParentFailed      → runMount after log.Fatalf; exits non-zero
//   DaemonStarting    → runDaemon before gofs.Mount
//   DaemonReady       → runDaemon after gofs.Mount and PID file written
//   DaemonStopping    → runDaemon after signal and cancel(); before Unmount
//   DaemonFlushing    → runDaemon after server.Unmount(); before persistFn(flushCtx)
//   DaemonStopped     → runDaemon after persistFn and os.Remove(pidPath)

module mount {

  // --------------------------------------------------------------------------
  // Types
  // --------------------------------------------------------------------------

  type ParentState =
    | ParentForking    // setting up pipe and starting daemon process
    | ParentWaiting    // blocking on pipe read; daemon still initialising
    | ParentDone       // received "ready\n"; exited 0
    | ParentFailed     // pipe closed without "ready\n"; exited non-zero

  type DaemonState =
    | DaemonStarting   // loading config, connecting to store, loading index
    | DaemonReady      // gofs.Mount succeeded; PID file written; serving FUSE
    | DaemonStopping   // SIGTERM/SIGINT received; background ctx cancelled; FUSE unmounting
    | DaemonFlushing   // FUSE unmounted; flushing index with fresh context
    | DaemonStopped    // index flushed; PID file removed; process exited

  // --------------------------------------------------------------------------
  // State
  // --------------------------------------------------------------------------

  var parentState:    ParentState
  var daemonState:    DaemonState
  var pipeSignalled:  bool  // daemon has written "ready\n" and closed pipe
  var pidFileExists:  bool  // <daemonDir>/daemon.pid is on disk
  var fsAccessible:   bool  // FUSE filesystem is usable by other processes
  var indexFlushed:   bool  // final index persist completed on this shutdown

  // --------------------------------------------------------------------------
  // Initialisation
  // --------------------------------------------------------------------------

  action init = {
    parentState'   = ParentForking
    daemonState'   = DaemonStarting
    pipeSignalled' = false
    pidFileExists' = false
    fsAccessible'  = false
    indexFlushed'  = false
  }

  // --------------------------------------------------------------------------
  // Actions
  // --------------------------------------------------------------------------

  // Parent finishes forking the daemon and starts waiting on the pipe.
  action parentStartsWaiting = {
    require(parentState == ParentForking)
    parentState' = ParentWaiting
  }

  // Daemon successfully completes gofs.Mount.
  // It writes the PID file, then writes "ready\n" to the pipe and closes it.
  // Both happen atomically from the model's perspective: the parent cannot
  // read from the pipe before the PID file is on disk.
  action daemonMountsSuccessfully = {
    require(daemonState == DaemonStarting)
    daemonState'   = DaemonReady
    fsAccessible'  = true
    pidFileExists' = true
    pipeSignalled' = true
  }

  // Daemon fails before or during gofs.Mount.
  // It closes the pipe without writing "ready\n" and exits.
  action daemonMountFails = {
    require(daemonState == DaemonStarting)
    daemonState'   = DaemonStopped
    fsAccessible'  = false
    pidFileExists' = false
    pipeSignalled' = false
  }

  // Parent reads "ready\n" from the pipe and exits 0.
  // Precondition: pipe was signalled (daemon wrote "ready\n" and closed it).
  action parentSucceeds = {
    require(parentState == ParentWaiting)
    require(pipeSignalled)
    parentState' = ParentDone
  }

  // Parent reads EOF from the pipe without receiving "ready\n" and exits non-zero.
  action parentFails = {
    require(parentState == ParentWaiting)
    require(not(pipeSignalled))
    require(daemonState == DaemonStopped) // pipe closed because daemon exited
    parentState' = ParentFailed
  }

  // A signal (SIGTERM or SIGINT) arrives while the daemon is serving.
  // The daemon cancels its background context (stopping the periodic persist
  // goroutine) and begins unmounting the FUSE server.
  action daemonReceivesSignal = {
    require(daemonState == DaemonReady)
    daemonState' = DaemonStopping
  }

  // `fusey unmount` finds the PID file, resolves the PID, and sends SIGTERM.
  // This is the same outcome as daemonReceivesSignal but named for clarity.
  action unmountCommandSendsSignal = {
    require(daemonState == DaemonReady)
    require(pidFileExists)
    daemonState' = DaemonStopping
  }

  // The FUSE server finishes unmounting. The kernel can no longer issue new
  // VFS requests so fsAccessible becomes false. The daemon then creates a
  // fresh context (not the cancelled background one) and begins the index flush.
  action daemonUnmounts = {
    require(daemonState == DaemonStopping)
    daemonState'  = DaemonFlushing
    fsAccessible' = false
  }

  // The final index flush completes (active chunk + index written to remote
  // store under the fresh flush context). The daemon removes the PID file
  // and exits cleanly.
  action daemonFlushes = {
    require(daemonState == DaemonFlushing)
    daemonState'   = DaemonStopped
    indexFlushed'  = true
    pidFileExists' = false
  }

  // --------------------------------------------------------------------------
  // Invariants
  // --------------------------------------------------------------------------

  // The parent only exits successfully when the daemon signalled readiness.
  invariant parentDoneImpliesSignalled =
    parentState == ParentDone implies pipeSignalled

  // The filesystem is accessible only when the daemon is actively serving.
  invariant fsAccessibleIffDaemonReady =
    fsAccessible iff daemonState == DaemonReady

  // The PID file exists while the daemon is serving, stopping, or flushing so
  // that `fusey unmount` can always find a daemon that has not yet cleaned up.
  invariant pidFileExistsWhileReachable =
    pidFileExists implies (daemonState == DaemonReady or
                           daemonState == DaemonStopping or
                           daemonState == DaemonFlushing)

  // When the daemon has stopped the PID file is gone.
  invariant pidFileGoneWhenStopped =
    daemonState == DaemonStopped implies not(pidFileExists)

  // The filesystem is never accessible after the daemon has stopped.
  invariant fsInaccessibleWhenStopped =
    daemonState == DaemonStopped implies not(fsAccessible)

  // The filesystem is not accessible while the daemon is flushing.
  // (FUSE was unmounted before the flush began.)
  invariant fsInaccessibleWhileFlushing =
    daemonState == DaemonFlushing implies not(fsAccessible)

  // The index is flushed before the daemon declares itself stopped.
  // This is the key safety property that the flushCtx fix enforces:
  // a cancelled context must never prevent the final remote write.
  invariant indexFlushedOnStop =
    daemonState == DaemonStopped implies indexFlushed

  // --------------------------------------------------------------------------
  // Temporal properties
  // --------------------------------------------------------------------------

  // The parent eventually resolves (exits 0 or non-zero).
  temporal parentEventuallyResolves =
    always(parentState == ParentWaiting implies
      eventually(parentState == ParentDone or parentState == ParentFailed))

  // A daemon that is stopping eventually completes its flush and exits.
  temporal daemonEventuallyStops =
    always(daemonState == DaemonStopping implies eventually(daemonState == DaemonStopped))

  // A daemon that is flushing eventually exits.
  temporal daemonEventuallyExitsAfterFlush =
    always(daemonState == DaemonFlushing implies eventually(daemonState == DaemonStopped))

  // Once the daemon is ready, it eventually receives a shutdown signal.
  temporal daemonEventuallyReceivesSignal =
    always(daemonState == DaemonReady implies
      eventually(daemonState == DaemonStopping or daemonState == DaemonStopped))

}
