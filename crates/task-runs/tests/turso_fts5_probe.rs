//! R474-S3 — Probe Turso FTS5 support.
//!
//! Tries CREATE VIRTUAL TABLE … USING fts5(...), INSERT, MATCH against a
//! turso 0.6.1 local database. Reports the observed status via stderr so
//! the W195 capability note is grounded in a real run.
//!
//! Lives in `task-runs` (not `app/yah/cli`) only because cli's tree has a
//! pre-existing unrelated compile break — task-runs already pins the same
//! workspace turso 0.6.1 and compiles cleanly. The probe is otherwise
//! self-contained and has no task-runs dependencies.
//!
//! See `.yah/docs/working/W195-stateful-service-pattern.md`
//! §Turso capability boundaries for the recorded finding.

use turso::Builder;

#[tokio::test]
async fn fts5_support_status() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("fts5_probe.turso");
    let db = Builder::new_local(path.to_str().unwrap())
        .build()
        .await
        .expect("open turso db");
    let conn = db.connect().expect("connect");

    let create = conn
        .execute_batch("CREATE VIRTUAL TABLE docs USING fts5(body);")
        .await;

    eprintln!("FTS5 CREATE VIRTUAL TABLE result: {:?}", create);

    let create_err = create.err().map(|e| format!("{e}"));
    if let Some(err) = create_err {
        eprintln!("turso 0.6.1 FTS5 status: UNSUPPORTED — {err}");
        return;
    }

    let insert = conn
        .execute("INSERT INTO docs(body) VALUES ('the quick brown fox')", ())
        .await;
    eprintln!("FTS5 INSERT result: {:?}", insert);
    let insert_err = insert.err().map(|e| format!("{e}"));

    let query = conn
        .query("SELECT body FROM docs WHERE docs MATCH 'fox'", ())
        .await;

    let mut match_count = 0usize;
    let mut match_err = None;
    match query {
        Ok(mut rows) => loop {
            match rows.next().await {
                Ok(Some(_)) => match_count += 1,
                Ok(None) => break,
                Err(e) => {
                    match_err = Some(format!("{e}"));
                    break;
                }
            }
        },
        Err(e) => match_err = Some(format!("{e}")),
    }

    eprintln!(
        "turso 0.6.1 FTS5 status: CREATE ok; INSERT err={:?}; MATCH err={:?}; rows={match_count}",
        insert_err, match_err
    );
}
