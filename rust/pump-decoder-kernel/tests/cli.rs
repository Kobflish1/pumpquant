use std::io::Write;
use std::process::{Command, Stdio};

fn run(arguments: &[&str], input: &[u8]) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pump-decoder-kernel"))
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("CLI should start");
    child
        .stdin
        .take()
        .expect("stdin should be piped")
        .write_all(input)
        .expect("fixture write should succeed");
    child.wait_with_output().expect("CLI should finish")
}

#[test]
fn jsonl_classifies_invalid_utf8_per_line() {
    let output = run(&["--jsonl"], b"\xff\nAA==\n");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("JSONL output should be UTF-8");
    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("\"error_kind\":\"malformed_transaction\""));
    assert!(lines[0].contains("not UTF-8"));
    assert!(lines[1].contains("\"error_kind\":\"malformed_transaction\""));
}

#[test]
fn jsonl_and_benchmark_are_rejected_together() {
    let output = run(&["--jsonl", "--benchmark"], b"");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8(output.stderr)
        .expect("stderr should be UTF-8")
        .contains("mutually exclusive"));
}
