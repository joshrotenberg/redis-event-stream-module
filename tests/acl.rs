//! Custom `@eventstream` ACL category (issue #69). `RM_AddACLCategory` is
//! Redis 7.4+; the module registers the category and tags all three module
//! commands into it (`EVENTSTREAM.STATS`, `EVENTSTREAM.STREAMS`, and the
//! write command `EVENTSTREAM.PRUNE`) where the API exists, and skips cleanly
//! on 7.2/7.3 (the null-pointer gate, same class as #45) so the load never
//! fails over ACL wiring. These tests derive the expected behavior empirically from the
//! module's own load-time notice rather than parsing versions, so they cover
//! every lane in the CI matrix (Redis 7.2/7.4/8, Valkey 8): whichever server
//! exposes the API gets the category, the rest get the fallback.

mod common;

use common::*;

/// Whether this server exposed `RM_AddACLCategory` to the module at load. The
/// module logs a single notice on the fallback path (any server where the API
/// pointer is null); its absence means the category is active. Keying on the
/// module's actual runtime decision keeps the assertion honest across lanes
/// without a version-string gate (the HEXPIRE-style capability probe from
/// issue #93, adapted to a feature with no user-facing command to probe).
fn category_registered(s: &TestServer) -> bool {
    !s.log().contains("predates RM_AddACLCategory")
}

/// `ACL CAT` as a list of category names.
fn acl_categories(conn: &mut redis::Connection) -> Vec<String> {
    redis::cmd("ACL").arg("CAT").query(conn).expect("ACL CAT")
}

#[test]
fn eventstream_category_listed_when_supported() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();
    if !category_registered(&s) {
        eprintln!("skipping: server predates RM_AddACLCategory (Redis 7.4+)");
        return;
    }
    let cats = acl_categories(&mut c);
    assert!(
        cats.iter().any(|cat| cat == "eventstream"),
        "ACL CAT must list the custom eventstream category on a supporting server: {cats:?}"
    );
}

#[test]
fn category_grants_all_commands_when_supported() {
    let s = TestServer::start(&[]);
    let mut c = s.conn();
    if !category_registered(&s) {
        eprintln!("skipping: server predates RM_AddACLCategory (Redis 7.4+)");
        return;
    }

    // A user whose only grant is +@eventstream (from a clean -@all slate) can
    // run all three module commands: proof the category actually carries
    // them, the group-level grant the issue is about.
    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("es")
        .arg("on")
        .arg(">pw")
        .arg("-@all")
        .arg("+@eventstream")
        .query(&mut c)
        .expect("SETUSER +@eventstream");

    // GETUSER reflects the category grant in the user's command rules.
    let commands: String = {
        let getuser: Vec<redis::Value> = redis::cmd("ACL")
            .arg("GETUSER")
            .arg("es")
            .query(&mut c)
            .expect("ACL GETUSER");
        // GETUSER is a flat [field, value, ...] map; pull the "commands" value.
        let flat: Vec<String> = getuser
            .iter()
            .map(|v| match v {
                redis::Value::SimpleString(s) => s.clone(),
                redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                other => format!("{other:?}"),
            })
            .collect();
        let idx = flat
            .iter()
            .position(|f| f == "commands")
            .expect("GETUSER has a commands field");
        flat[idx + 1].clone()
    };
    assert!(
        commands.contains("+@eventstream"),
        "ACL GETUSER commands must show the +@eventstream grant: {commands}"
    );

    // Authenticating as that user, both commands succeed and a non-module
    // command stays denied — the grant is scoped to the module's commands.
    let mut uc = s.conn();
    let _: () = redis::cmd("AUTH")
        .arg("es")
        .arg("pw")
        .query(&mut uc)
        .expect("AUTH es");
    let _: redis::Value = redis::cmd("EVENTSTREAM.STATS")
        .query(&mut uc)
        .expect("EVENTSTREAM.STATS allowed via @eventstream");
    let _: redis::Value = redis::cmd("EVENTSTREAM.STREAMS")
        .query(&mut uc)
        .expect("EVENTSTREAM.STREAMS allowed via @eventstream");
    // The category also carries the write command (SPEC.md section 8): a
    // regression dropping PRUNE from the tagging would pass the two calls
    // above but fail here.
    let _: i64 = redis::cmd("EVENTSTREAM.PRUNE")
        .query(&mut uc)
        .expect("EVENTSTREAM.PRUNE allowed via @eventstream");
    let denied: Result<redis::Value, _> = redis::cmd("GET").arg("k").query(&mut uc);
    assert!(
        denied.is_err(),
        "a +@eventstream-only user must not be able to run GET"
    );
}

#[test]
fn loads_and_commands_individually_grantable_without_category() {
    // The fallback lane (Redis 7.2/7.3, or any server with a null API pointer):
    // the module must load cleanly and the commands remain grantable by name,
    // exactly as before the category existed. The name grants mirror the
    // category's full three-command equivalent (SPEC.md section 8 and the
    // module's own load-time notice). Skips where the category is present so
    // the same test body runs meaningfully on every lane.
    let s = TestServer::start(&[]);
    let mut c = s.conn();
    if category_registered(&s) {
        eprintln!("skipping: server supports the @eventstream category");
        return;
    }

    // Loaded cleanly: the command answers.
    let _: redis::Value = redis::cmd("EVENTSTREAM.STATS")
        .query(&mut c)
        .expect("EVENTSTREAM.STATS works on a server without the ACL API");

    // No phantom category on this server.
    assert!(
        !acl_categories(&mut c)
            .iter()
            .any(|cat| cat == "eventstream"),
        "no eventstream category may exist where RM_AddACLCategory is absent"
    );

    // The documented per-command workaround still holds.
    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("es")
        .arg("on")
        .arg(">pw")
        .arg("-@all")
        .arg("+eventstream.stats")
        .arg("+eventstream.streams")
        .arg("+eventstream.prune")
        .query(&mut c)
        .expect("SETUSER with explicit per-command grants");
    let mut uc = s.conn();
    let _: () = redis::cmd("AUTH")
        .arg("es")
        .arg("pw")
        .query(&mut uc)
        .expect("AUTH es");
    let _: redis::Value = redis::cmd("EVENTSTREAM.STATS")
        .query(&mut uc)
        .expect("EVENTSTREAM.STATS allowed via explicit grant");
    let _: redis::Value = redis::cmd("EVENTSTREAM.STREAMS")
        .query(&mut uc)
        .expect("EVENTSTREAM.STREAMS allowed via explicit grant");
    let _: i64 = redis::cmd("EVENTSTREAM.PRUNE")
        .query(&mut uc)
        .expect("EVENTSTREAM.PRUNE allowed via explicit grant");
}
