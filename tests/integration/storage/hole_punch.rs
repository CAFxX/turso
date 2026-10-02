use crate::common::{do_flush, run_query, run_query_on_row, TempDatabase};
use rand::{rng, RngCore};
use turso_core::Row;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[cfg(unix)]
fn hole_punch_supported(
    io: &std::sync::Arc<dyn turso_core::io::IO + Send>,
    db_path: &std::path::Path,
) -> bool {
    use std::io::Write;
    // Probe on the same filesystem as the database: support is a filesystem property.
    let probe_path = db_path.with_extension("holepunch-probe");
    let mut probe = std::fs::File::create(&probe_path).unwrap();
    probe.write_all(&[0xA5u8; 8192]).unwrap();
    probe.flush().unwrap();
    drop(probe);
    let file = io
        .open_file(
            probe_path.to_str().unwrap(),
            turso_core::OpenFlags::Create,
            false,
        )
        .unwrap();
    let supported = file.punch_hole(0, 4096).is_ok();
    drop(file);
    std::fs::remove_file(&probe_path).ok();
    supported
}

#[test]
fn test_hole_punch_on_delete_wal_checkpoint_reopen() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_name = format!("test-hole-punch-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);
    let db_path = tmp_db.path.clone();

    let conn = tmp_db.connect_limbo();
    run_query(&tmp_db, &conn, "PRAGMA journal_mode=WAL;")?;
    run_query(
        &tmp_db,
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);",
    )?;
    for _ in 0..20 {
        let mut insert = String::from("INSERT INTO t(v) VALUES ");
        for i in 0..250 {
            if i > 0 {
                insert.push(',');
            }
            insert.push_str(&format!("('value-{i:05}-with-padding-to-span-pages')"));
        }
        insert.push(';');
        run_query(&tmp_db, &conn, &insert)?;
    }
    do_flush(&conn, &tmp_db)?;
    // Checkpoint to backfill the WAL into the DB file, so blocks_before
    // measures the fully populated database.
    run_query(&tmp_db, &conn, "PRAGMA wal_checkpoint(TRUNCATE);")?;

    #[cfg(unix)]
    let blocks_before = std::fs::metadata(&db_path)?.blocks();

    run_query(&tmp_db, &conn, "DELETE FROM t WHERE id > 100;")?;
    do_flush(&conn, &tmp_db)?;
    run_query(&tmp_db, &conn, "PRAGMA wal_checkpoint(TRUNCATE);")?;

    run_query_on_row(&tmp_db, &conn, "PRAGMA integrity_check;", |row: &Row| {
        assert_eq!(row.get::<String>(0).unwrap(), "ok");
    })?;

    #[cfg(unix)]
    if hole_punch_supported(&tmp_db.io, &db_path) {
        let blocks_after = std::fs::metadata(&db_path)?.blocks();
        assert!(
            blocks_after < blocks_before,
            "expected freed pages to release blocks: {blocks_after} < {blocks_before}"
        );
    }
    drop(conn);

    let tmp_db = TempDatabase::new_with_existent(&db_path);
    let conn = tmp_db.connect_limbo();
    run_query_on_row(&tmp_db, &conn, "PRAGMA integrity_check;", |row: &Row| {
        assert_eq!(row.get::<String>(0).unwrap(), "ok");
    })?;
    run_query_on_row(
        &tmp_db,
        &conn,
        "SELECT COUNT(*), MIN(id), MAX(id) FROM t;",
        |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 100);
            assert_eq!(row.get::<i64>(1).unwrap(), 1);
            assert_eq!(row.get::<i64>(2).unwrap(), 100);
        },
    )?;
    run_query_on_row(
        &tmp_db,
        &conn,
        "SELECT v FROM t WHERE id = 42;",
        |row: &Row| {
            assert_eq!(
                row.get::<String>(0).unwrap(),
                "value-00041-with-padding-to-span-pages"
            );
        },
    )?;
    Ok(())
}

#[test]
fn test_hole_punch_skipped_with_synchronous_off() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_name = format!("test-hole-punch-sync-off-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);
    let db_path = tmp_db.path.clone();

    let conn = tmp_db.connect_limbo();
    run_query(&tmp_db, &conn, "PRAGMA journal_mode=WAL;")?;
    run_query(
        &tmp_db,
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);",
    )?;
    for _ in 0..20 {
        let mut insert = String::from("INSERT INTO t(v) VALUES ");
        for i in 0..250 {
            if i > 0 {
                insert.push(',');
            }
            insert.push_str(&format!("('value-{i:05}-with-padding-to-span-pages')"));
        }
        insert.push(';');
        run_query(&tmp_db, &conn, &insert)?;
    }
    do_flush(&conn, &tmp_db)?;
    // Checkpoint to backfill the WAL into the DB file, so blocks_before
    // measures the fully populated database.
    run_query(&tmp_db, &conn, "PRAGMA wal_checkpoint(TRUNCATE);")?;

    // synchronous=OFF skips the WAL fsync that hole punching relies on for
    // crash safety, so the checkpoint must not punch.
    run_query(&tmp_db, &conn, "PRAGMA synchronous=OFF;")?;

    #[cfg(unix)]
    let blocks_before = std::fs::metadata(&db_path)?.blocks();

    run_query(&tmp_db, &conn, "DELETE FROM t WHERE id > 100;")?;
    do_flush(&conn, &tmp_db)?;
    run_query(&tmp_db, &conn, "PRAGMA wal_checkpoint(TRUNCATE);")?;

    run_query_on_row(&tmp_db, &conn, "PRAGMA integrity_check;", |row: &Row| {
        assert_eq!(row.get::<String>(0).unwrap(), "ok");
    })?;

    #[cfg(unix)]
    if hole_punch_supported(&tmp_db.io, &db_path) {
        let blocks_after = std::fs::metadata(&db_path)?.blocks();
        assert_eq!(
            blocks_after, blocks_before,
            "synchronous=OFF must skip hole punching"
        );
    }
    Ok(())
}

#[test]
fn test_hole_punch_reused_page_not_punched_across_connections() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_name = format!("test-hole-punch-reuse-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);

    let conn_a = tmp_db.connect_limbo();
    let conn_b = tmp_db.connect_limbo();
    run_query(&tmp_db, &conn_a, "PRAGMA journal_mode=WAL;")?;
    run_query(
        &tmp_db,
        &conn_a,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);",
    )?;
    for _ in 0..20 {
        let mut insert = String::from("INSERT INTO t(v) VALUES ");
        for i in 0..250 {
            if i > 0 {
                insert.push(',');
            }
            insert.push_str(&format!("('value-{i:05}-with-padding-to-span-pages')"));
        }
        insert.push(';');
        run_query(&tmp_db, &conn_a, &insert)?;
    }
    do_flush(&conn_a, &tmp_db)?;
    run_query(&tmp_db, &conn_a, "PRAGMA wal_checkpoint(TRUNCATE);")?;

    // Conn A frees pages; the ranges are queued but not yet punched.
    run_query(&tmp_db, &conn_a, "DELETE FROM t WHERE id > 100;")?;
    do_flush(&conn_a, &tmp_db)?;

    // Conn B reuses the freed pages before the checkpoint runs. The reuse
    // must excise those pages from the shared punch queue.
    for _ in 0..10 {
        let mut insert = String::from("INSERT INTO t(v) VALUES ");
        for i in 0..250 {
            if i > 0 {
                insert.push(',');
            }
            insert.push_str(&format!("('reused-{i:05}-with-padding-to-span-pages')"));
        }
        insert.push(';');
        run_query(&tmp_db, &conn_b, &insert)?;
    }
    do_flush(&conn_b, &tmp_db)?;

    // Checkpoint now: must not punch pages that B reused for live data.
    run_query(&tmp_db, &conn_a, "PRAGMA wal_checkpoint(TRUNCATE);")?;

    run_query_on_row(&tmp_db, &conn_a, "PRAGMA integrity_check;", |row: &Row| {
        assert_eq!(row.get::<String>(0).unwrap(), "ok");
    })?;
    // B's reused rows must be intact: no live page was punched.
    run_query_on_row(
        &tmp_db,
        &conn_b,
        "SELECT COUNT(*) FROM t WHERE v LIKE 'reused-%';",
        |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 2500);
        },
    )?;
    run_query_on_row(
        &tmp_db,
        &conn_b,
        "SELECT v FROM t WHERE v = 'reused-00123-with-padding-to-span-pages';",
        |row: &Row| {
            assert_eq!(
                row.get::<String>(0).unwrap(),
                "reused-00123-with-padding-to-span-pages"
            );
        },
    )?;
    Ok(())
}

#[test]
fn test_hole_punch_old_reader_sees_consistent_data() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let db_name = format!("test-hole-punch-reader-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);

    let conn_a = tmp_db.connect_limbo();
    let conn_b = tmp_db.connect_limbo();
    run_query(&tmp_db, &conn_a, "PRAGMA journal_mode=WAL;")?;
    run_query(
        &tmp_db,
        &conn_a,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);",
    )?;
    for _ in 0..20 {
        let mut insert = String::from("INSERT INTO t(v) VALUES ");
        for i in 0..250 {
            if i > 0 {
                insert.push(',');
            }
            insert.push_str(&format!("('value-{i:05}-with-padding-to-span-pages')"));
        }
        insert.push(';');
        run_query(&tmp_db, &conn_a, &insert)?;
    }
    do_flush(&conn_a, &tmp_db)?;
    run_query(&tmp_db, &conn_a, "PRAGMA wal_checkpoint(TRUNCATE);")?;

    // Conn B opens a read transaction, pinning its snapshot.
    run_query(&tmp_db, &conn_b, "BEGIN;")?;
    run_query_on_row(&tmp_db, &conn_b, "SELECT COUNT(*) FROM t;", |row: &Row| {
        assert_eq!(row.get::<i64>(0).unwrap(), 5000);
    })?;

    // Conn A frees pages, commits, and checkpoints while B's read
    // transaction is still open.
    run_query(&tmp_db, &conn_a, "DELETE FROM t WHERE id > 100;")?;
    do_flush(&conn_a, &tmp_db)?;
    run_query(&tmp_db, &conn_a, "PRAGMA wal_checkpoint(TRUNCATE);")?;

    // B must still see its pinned snapshot, unaffected by the checkpoint.
    run_query_on_row(
        &tmp_db,
        &conn_b,
        "SELECT COUNT(*), MIN(id), MAX(id) FROM t;",
        |row: &Row| {
            assert_eq!(row.get::<i64>(0).unwrap(), 5000);
            assert_eq!(row.get::<i64>(1).unwrap(), 1);
            assert_eq!(row.get::<i64>(2).unwrap(), 5000);
        },
    )?;
    run_query(&tmp_db, &conn_b, "COMMIT;")?;

    run_query_on_row(&tmp_db, &conn_a, "PRAGMA integrity_check;", |row: &Row| {
        assert_eq!(row.get::<String>(0).unwrap(), "ok");
    })?;
    Ok(())
}
