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
//
// Mount log:
//   <daemonDir>/mount.log is opened by runMount and assigned as the daemon
//   subprocess's stderr before cmd.Start. Anything the daemon — and any helper
//   it execs (notably go-fuse's fusermount3 fallback) — writes to fd 2 ends
//   up there. This is what makes the user's `fusey mount` failure diagnostic:
//   instead of go-fuse's cryptic "fusermount exited with code 256" you also
//   get the kernel syscall error (EPERM, EACCES, EBUSY, ...) in mount.log.
//
// Configuration (resolved from environment variables at startup):
//   FUSEY_ALLOW_OTHER    — controls the FUSE `allow_other` mount option
//                          (default `true`). When `true`, fusey asks the
//                          kernel for an `allow_other` mount so any user on
//                          the host may access the mountpoint; when `false`,
//                          only the UID that called `mount(2)` (the fusey
//                          daemon) may access it — the FUSE default. fusey
//                          defaults this to `true` because the typical
//                          deployment runs the daemon as root (the mounter
//                          needs `CAP_SYS_ADMIN`) while the consumer process
//                          (e.g. an agent-server) runs as a non-root user;
//                          without `allow_other` the kernel rejects the
//                          consumer's VFS operations before fusey's own
//                          permission code ever runs.
//
//   This spec does NOT model the lifecycle differently based on
//   FUSEY_ALLOW_OTHER: the mount option is consumed by gofs.Mount at the
//   DaemonStarting → DaemonReady transition and either succeeds (the
//   mount is operational with the requested option) or fails
//   (`daemonMountFails`) — and that second case covers, among other
//   failure modes, the situation where the kernel refuses `allow_other`
//   because `/etc/fuse.conf` does not enable `user_allow_other`. The
//   `ALLOW_OTHER` constant below is the spec-level projection of the
//   env var; invariants in this file hold for both values.
//
// CLI dispatch surface:
//
//   cmd/fusey/main.go's main() dispatches on os.Args[1] (with `--version`/
//   `-v` checked before any other argument validation). The full surface it
//   recognises, as of this writing, is:
//
//     Subcommand     Handler / path              Modeled by this spec?
//     -----------    -----------------------     ---------------------
//     mount          runMount                    YES (parent/daemon lifecycle below)
//     daemon         runDaemon (internal)        YES (parent/daemon lifecycle below)
//     unmount        runUnmount                  YES (signals the daemon via unmountCommandSendsSignal)
//     compact        runCompact                  NO  — background maintenance; out of scope here
//     version        print Version and exit 0    NO  — CLI introspection; pure side-effect-free printer
//
//     Top-level flag Handler                     Modeled by this spec?
//     -------------  -----------------------     ---------------------
//     --version      print Version and exit 0    NO  — same path as `version` subcommand
//     -v             print Version and exit 0    NO  — same path as `version` subcommand
//
//   The `compact`, `version`, `--version`, and `-v` paths are intentionally NOT
//   modeled by this spec: they do not transition any state in the parent/daemon
//   lifecycle. Their non-interference with the modeled invariants is enforced
//   structurally — `cmd/fusey/main_test.go::TestVersionSubcommand` and the
//   (existing) subprocess-based integration tests cover the Go side, and the
//   dispatch arms `return` before this spec's `init` action (or any lifecycle
//   action) is ever reached.
//
//   Should a future change ever couple one of these paths to the lifecycle
//   (e.g. by having `compact` re-flush through this state machine, or by making
//   `version` consult on-disk state), the addition here is where the
//   corresponding quint action, invariant, and state variable would land.

module mount {

  // --------------------------------------------------------------------------
  // Constants
  // --------------------------------------------------------------------------

  // Spec-level projection of the FUSEY_ALLOW_OTHER environment variable:
  // when `true`, the daemon requests a FUSE `allow_other` mount so any user
  // on the host may access the mountpoint; when `false`, only the UID that
  // called `mount(2)` may access it. Modeled as a constant (not a state
  // variable) because the value is read once from the environment at
  // startup and does not change during the daemon's lifetime. See the
  // preamble "Configuration" section for the rationale.
  const ALLOW_OTHER: bool

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
  var mountLogExists: bool  // <daemonDir>/mount.log has been opened by runMount
                            // and (if the daemon ran) populated with the
                            // daemon's stderr via the fd-2 inheritance trick

  // --------------------------------------------------------------------------
  // Initialisation
  // --------------------------------------------------------------------------

  action init = {
    parentState'    = ParentForking
    daemonState'    = DaemonStarting
    pipeSignalled'  = false
    pidFileExists'  = false
    fsAccessible'   = false
    indexFlushed'   = false
    mountLogExists' = false
  }

  // --------------------------------------------------------------------------
  // Actions
  // --------------------------------------------------------------------------

  // Parent finishes forking the daemon and starts waiting on the pipe.
  // In runMount, the mount.log sidecar is opened before cmd.Start, so it
  // exists on disk from the moment the parent is waiting for readiness.
  action parentStartsWaiting = {
    require(parentState == ParentForking)
    parentState'    = ParentWaiting
    mountLogExists' = true
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

  // When the parent reports failure the mount log is on disk: this is the
  // whole point of the sidecar fd-2 capture — it is the only place the user
  // can read the kernel syscall error that caused the failure.
  invariant parentFailedImpliesMountLog =
    parentState == ParentFailed implies mountLogExists

  // The mount log is created before the daemon is forked, so it exists
  // throughout the waiting phase regardless of whether the daemon ultimately
  // succeeds or fails.
  invariant mountLogExistsWhileParentWaiting =
    parentState == ParentWaiting implies mountLogExists

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
