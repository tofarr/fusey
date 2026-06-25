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
// Correspondence to Go implementation (cmd/fusey/main.go):
//   ParentForking     → runMount before cmd.Start
//   ParentWaiting     → runMount blocking on io.ReadAll(r)
//   ParentDone        → runMount after fmt.Printf; exits 0
//   ParentFailed      → runMount after log.Fatalf; exits non-zero
//   DaemonStarting    → runDaemon before gofs.Mount
//   DaemonReady       → runDaemon after gofs.Mount and PID file written
//   DaemonStopping    → runDaemon after signal received
//   DaemonStopped     → runDaemon after server.Unmount and os.Remove(pidPath)

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
    | DaemonStopping   // SIGTERM/SIGINT received; running unmount
    | DaemonStopped    // unmount complete; PID file removed; process exited

  // --------------------------------------------------------------------------
  // State
  // --------------------------------------------------------------------------

  var parentState:    ParentState
  var daemonState:    DaemonState
  var pipeSignalled:  bool  // daemon has written "ready\n" and closed pipe
  var pidFileExists:  bool  // <daemonDir>/daemon.pid is on disk
  var fsAccessible:   bool  // FUSE filesystem is usable by other processes

  // --------------------------------------------------------------------------
  // Initialisation
  // --------------------------------------------------------------------------

  action init = {
    parentState'   = ParentForking
    daemonState'   = DaemonStarting
    pipeSignalled' = false
    pidFileExists' = false
    fsAccessible'  = false
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

  // A signal (SIGTERM or SIGINT) triggers daemon shutdown.
  // Only possible while the daemon is serving (DaemonReady).
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

  // Daemon finishes unmounting and cleans up.
  action daemonStops = {
    require(daemonState == DaemonStopping)
    daemonState'  = DaemonStopped
    fsAccessible' = false
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

  // The PID file exists while the daemon is serving or shutting down so that
  // `fusey unmount` can always find a ready daemon.
  invariant pidFileExistsWhileReachable =
    pidFileExists implies (daemonState == DaemonReady or daemonState == DaemonStopping)

  // When the daemon has stopped the PID file is gone.
  invariant pidFileGoneWhenStopped =
    daemonState == DaemonStopped implies not(pidFileExists)

  // The filesystem is never accessible after the daemon has stopped.
  invariant fsInaccessibleWhenStopped =
    daemonState == DaemonStopped implies not(fsAccessible)

  // --------------------------------------------------------------------------
  // Temporal properties
  // --------------------------------------------------------------------------

  // The parent eventually resolves (exits 0 or non-zero).
  temporal parentEventuallyResolves =
    always(parentState == ParentWaiting implies
      eventually(parentState == ParentDone or parentState == ParentFailed))

  // A signalled daemon eventually stops.
  temporal daemonEventuallyStops =
    always(daemonState == DaemonStopping implies eventually(daemonState == DaemonStopped))

  // Once the daemon is ready, it eventually receives a shutdown signal.
  temporal daemonEventuallyReceivesSignal =
    always(daemonState == DaemonReady implies
      eventually(daemonState == DaemonStopping or daemonState == DaemonStopped))

}
