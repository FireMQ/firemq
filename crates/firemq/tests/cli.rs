//! The `firemq` command-line tool against an embedded broker.

use std::process::{Command, Output};

#[derive(Clone)]
struct Urls {
    http: String,
    fire: String,
}

fn urls(server: &firemq::Server) -> Urls {
    Urls {
        http: server.http_url().unwrap(),
        fire: server.fire_url().unwrap(),
    }
}

fn firemq(u: &Urls, args: &[&str], stdin: Option<&str>) -> Output {
    use std::io::Write;
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_firemq"));
    cmd.args(args)
        .env("FIREMQ_HTTP_URL", &u.http)
        .env("FIREMQ_URL", &u.fire)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    if let Some(input) = stdin {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    } else {
        drop(child.stdin.take());
    }
    child.wait_with_output().unwrap()
}

fn ok(out: Output) -> String {
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        out.status.success(),
        "command failed:\nstdout: {stdout}\nstderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    stdout + &String::from_utf8_lossy(&out.stderr)
}

/// Runs the CLI on a blocking thread so the in-process broker keeps serving.
async fn exec(u: &Urls, args: &[&str], stdin: Option<&str>) -> Output {
    let u = u.clone();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let stdin = stdin.map(String::from);
    tokio::task::spawn_blocking(move || {
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        firemq(&u, &refs, stdin.as_deref())
    })
    .await
    .unwrap()
}

async fn run(server: &firemq::Server, args: &[&str]) -> String {
    ok(exec(&urls(server), args, None).await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topology_publish_consume_and_admin_from_the_cli() {
    let server = firemq::builder()
        .ephemeral()
        .random_ports()
        .start()
        .await
        .unwrap();
    run(
        &server,
        &["ctl", "declare-queue", "jobs", "--arg", "x-max-length=100"],
    )
    .await;
    run(&server, &["ctl", "declare-exchange", "events", "topic"]).await;
    run(
        &server,
        &["ctl", "bind", "events", "jobs", "--key", "job.*"],
    )
    .await;
    let out = run(&server, &["pub", "events", "job.created", "{\"id\":1}"]).await;
    assert!(out.contains("(confirmed)"), "{out}");
    let out = run(
        &server,
        &["pub", "--queue", "jobs", "--count", "50", "bulk"],
    )
    .await;
    assert!(out.contains("published 50 messages"), "{out}");

    let list = run(&server, &["ctl", "list", "queues"]).await;
    assert!(
        list.lines()
            .any(|l| l.starts_with("jobs") && l.contains("51")),
        "{list}"
    );
    let bindings = run(&server, &["ctl", "list", "bindings", "--json"]).await;
    let v: serde_json::Value = serde_json::from_str(&bindings).unwrap();
    assert!(
        v.as_array()
            .unwrap()
            .iter()
            .any(|b| b["routing_key"] == "job.*")
    );

    let got = run(&server, &["sub", "jobs", "--count", "2"]).await;
    assert_eq!(
        got.lines().take(2).collect::<Vec<_>>(),
        ["{\"id\":1}", "bulk"]
    );
    let peek = run(&server, &["ctl", "get", "jobs"]).await;
    assert!(peek.contains("[(default) jobs] bulk"), "{peek}");

    run(
        &server,
        &["ctl", "add-user", "app", "pw", "--tags", "management"],
    )
    .await;
    run(
        &server,
        &["ctl", "set-permissions", "app", "--configure", "^app\\."],
    )
    .await;
    let perms = run(&server, &["ctl", "list", "permissions"]).await;
    assert!(
        perms
            .lines()
            .any(|l| l.starts_with("app") && l.contains("^app\\.")),
        "{perms}"
    );
    run(
        &server,
        &["ctl", "set-policy", "cap", "^jobs$", "{\"max-length\":10}"],
    )
    .await;
    let q = run(&server, &["ctl", "list", "queues"]).await;
    assert!(q.contains("cap"), "{q}");

    let exported = run(&server, &["ctl", "export"]).await;
    assert!(exported.contains("\"jobs\""));
    run(&server, &["ctl", "purge", "jobs"]).await;
    run(&server, &["ctl", "delete-queue", "jobs"]).await;
    let q = run(&server, &["ctl", "list", "queues"]).await;
    assert!(!q.contains("jobs"), "{q}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pub_reads_the_body_from_stdin_and_errors_are_clear() {
    let server = firemq::builder()
        .ephemeral()
        .random_ports()
        .start()
        .await
        .unwrap();
    run(&server, &["ctl", "declare-queue", "in"]).await;
    let u = urls(&server);
    ok(exec(&u, &["pub", "--queue", "in"], Some("from stdin")).await);
    let got = run(&server, &["sub", "in", "--count", "1"]).await;
    assert!(got.starts_with("from stdin"));
    let out = exec(&u, &["ctl", "list", "queues", "--pass", "wrong"], None).await;
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("login failed for user 'guest'"));
}

#[test]
fn init_writes_a_config_the_server_accepts() {
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_firemq"))
        .arg("init")
        .current_dir(dir.path())
        .output()
        .unwrap();
    assert!(out.status.success());
    let text = std::fs::read_to_string(dir.path().join("firemq.toml")).unwrap();
    assert!(text.contains("[listeners]"));
    let again = Command::new(env!("CARGO_BIN_EXE_firemq"))
        .arg("init")
        .current_dir(dir.path())
        .output()
        .unwrap();
    assert!(
        !again.status.success(),
        "init must not overwrite without --force"
    );
}

#[test]
fn completions_are_generated() {
    let out = Command::new(env!("CARGO_BIN_EXE_firemq"))
        .args(["completions", "bash"])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("firemq"));
}
