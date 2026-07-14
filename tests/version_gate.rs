//! Pre-7.2 load refusal, SPEC.md section 15 checklist item (issue #77). The
//! module requires `RM_AddPostNotificationJob` (Redis 7.2), and on a server
//! below 7.2 the load fails — today as a process abort inside the wrapper's
//! macro-generated registration (it unwraps 7.2-only API pointers before `init`
//! runs), which no local change can turn into a clean `Status::Err` (SPEC.md
//! section 14). CI has no pre-7.2 server (the matrix floor is Redis 7.2.8), so
//! the true refusal cannot be exercised here; the mocked-version variant lives
//! in the `version_supported` unit test (src/lib.rs). This integration test
//! pins the supported side of the gate: on the running server, which the
//! harness guarantees is >= 7.2, the module loads cleanly and the version
//! refusal never fires. Exercising the abort on a real Redis 7.0 is left to a
//! dedicated pre-7.2 harness lane (not in the CI matrix).

mod common;

use common::*;

#[test]
fn supported_server_loads_without_version_refusal() {
    let s = TestServer::start(&["events", "*"]);
    let mut c = s.conn();

    // The harness floor: the version gate only rejects below 7.2, so the whole
    // supported-side contract rests on the running server being >= 7.2. Record
    // it explicitly rather than assume it, so a future matrix change that drops
    // below the floor surfaces here instead of silently voiding the test.
    let (major, minor) = server_version(&mut c);
    assert!(
        (major, minor) >= (7, 2),
        "test harness must run Redis/Valkey 7.2+; got {major}.{minor}"
    );

    // The module loaded and captures, which means the required API resolved and
    // neither the version gate nor the `RM_AddPostNotificationJob` pointer check
    // (src/lib.rs init) refused. A load refusal would have aborted the process
    // and `TestServer::start` would have panicked before reaching here.
    expire_key_and_wait(&s, "session:abc", "events:expired", 0);

    // The version-refusal log line is exactly what a pre-7.2 server would emit
    // if the gate ever became reachable there; a supported server must never
    // log it.
    let log = s.log();
    assert!(
        !log.contains("requires Redis 7.2 or newer"),
        "supported server must not log the version refusal; log:\n{log}"
    );
    assert!(
        !log.contains("requires RedisModule_AddPostNotificationJob"),
        "supported server must not log the API-pointer refusal; log:\n{log}"
    );
}
