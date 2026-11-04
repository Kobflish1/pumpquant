use std::env;
use std::io::{self, BufRead};
use std::process::ExitCode;
use std::time::Instant;

use pump_decoder_kernel::{ParsedTransaction, SolanaWireError, parse_serialized_transaction};

fn usage() {
    println!(
        "pump-decoder-kernel\n\
         \n\
         Read one canonical Solana transaction base64 value per stdin line.\n\
         \n\
         Options:\n\
           --jsonl                 emit one JSON object per input line\n\
           --benchmark             repeat valid input lines and report throughput\n\
           --iterations N          benchmark repetitions (default: 1000)\n\
           --help                  show this help\n\
         \n\
         Default output is first_signature<TAB>static_key[,static_key...]."
    );
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write;
                write!(escaped, "\\u{:04x}", character as u32).expect("writing String cannot fail");
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn json_success(result: &ParsedTransaction) -> String {
    let keys = result
        .static_account_keys
        .iter()
        .map(|key| format!("\"{}\"", json_escape(key)))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"ok\":true,\"first_signature\":\"{}\",\"static_account_keys\":[{}]}}",
        json_escape(&result.first_signature),
        keys
    )
}

fn json_error(error: &SolanaWireError) -> String {
    let kind = match error {
        SolanaWireError::MalformedTransaction(_) => "malformed_transaction",
        SolanaWireError::UnsupportedTransactionVersion(_) => "unsupported_transaction_version",
    };
    json_error_parts(kind, &error.to_string())
}

fn json_error_parts(kind: &str, message: &str) -> String {
    format!(
        "{{\"ok\":false,\"error_kind\":\"{}\",\"error\":\"{}\"}}",
        kind,
        json_escape(message)
    )
}

fn read_input_lines() -> Result<Vec<Vec<u8>>, String> {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let mut lines = Vec::new();
    loop {
        let mut line = Vec::new();
        let byte_count = reader
            .read_until(b'\n', &mut line)
            .map_err(|error| format!("failed to read stdin: {error}"))?;
        if byte_count == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        lines.push(line);
    }
    Ok(lines)
}

fn run_benchmark(values: &[&str], iterations: usize) -> Result<(), String> {
    if values.is_empty() {
        return Err("benchmark input must contain at least one transaction".to_owned());
    }
    if iterations == 0 {
        return Err("benchmark iterations must be positive".to_owned());
    }
    let start = Instant::now();
    let mut checksum: u64 = 0;
    let mut total_parses: usize = 0;
    for _ in 0..iterations {
        for value in values {
            let parsed = parse_serialized_transaction(value)
                .map_err(|error| format!("benchmark input is invalid: {error}"))?;
            checksum = checksum
                .wrapping_add(parsed.first_signature.len() as u64)
                .wrapping_add(parsed.static_account_keys.len() as u64);
            for key in parsed.static_account_keys {
                checksum = checksum.wrapping_add(key.len() as u64);
            }
            total_parses += 1;
        }
    }
    let elapsed_seconds = start.elapsed().as_secs_f64();
    let throughput = total_parses as f64 / elapsed_seconds.max(f64::MIN_POSITIVE);
    println!(
        "benchmark records={} iterations={} total_parses={} elapsed_seconds={:.6} \
         throughput_transactions_per_second={:.2} checksum={}",
        values.len(),
        iterations,
        total_parses,
        elapsed_seconds,
        throughput,
        std::hint::black_box(checksum)
    );
    Ok(())
}

fn main() -> ExitCode {
    let mut jsonl = false;
    let mut benchmark = false;
    let mut iterations = 1_000usize;
    let arguments: Vec<String> = env::args().skip(1).collect();
    let mut argument_index = 0;
    while argument_index < arguments.len() {
        match arguments[argument_index].as_str() {
            "--jsonl" => jsonl = true,
            "--benchmark" => benchmark = true,
            "--iterations" => {
                argument_index += 1;
                let Some(value) = arguments.get(argument_index) else {
                    eprintln!("--iterations requires a positive integer");
                    return ExitCode::from(2);
                };
                iterations = match value.parse() {
                    Ok(value) => value,
                    Err(_) => {
                        eprintln!("--iterations requires a positive integer");
                        return ExitCode::from(2);
                    }
                };
            }
            "--help" | "-h" => {
                usage();
                return ExitCode::SUCCESS;
            }
            argument => {
                eprintln!("unknown option {argument:?}; use --help");
                return ExitCode::from(2);
            }
        }
        argument_index += 1;
    }
    if jsonl && benchmark {
        eprintln!("--jsonl and --benchmark are mutually exclusive");
        return ExitCode::from(2);
    }

    let input_lines = match read_input_lines() {
        Ok(lines) => lines,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };

    if benchmark {
        let values = match input_lines
            .iter()
            .map(|line| std::str::from_utf8(line))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(values) => values,
            Err(_) => {
                eprintln!("benchmark input contains invalid UTF-8");
                return ExitCode::from(1);
            }
        };
        return match run_benchmark(&values, iterations) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(1)
            }
        };
    }

    let mut failed = false;
    for line in input_lines {
        let value = match std::str::from_utf8(&line) {
            Ok(value) => value,
            Err(_) if jsonl => {
                println!(
                    "{}",
                    json_error_parts(
                        "malformed_transaction",
                        "serialized transaction base64 is not UTF-8",
                    )
                );
                continue;
            }
            Err(_) => {
                eprintln!("transaction decode failed: serialized transaction base64 is not UTF-8");
                failed = true;
                continue;
            }
        };
        match parse_serialized_transaction(value) {
            Ok(result) if jsonl => println!("{}", json_success(&result)),
            Ok(result) => println!(
                "{}\t{}",
                result.first_signature,
                result.static_account_keys.join(",")
            ),
            Err(error) if jsonl => println!("{}", json_error(&error)),
            Err(error) => {
                eprintln!("transaction decode failed: {error}");
                failed = true;
            }
        }
    }
    if failed {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
