//! `orionctl get status`: entries of the node's volatile status lane.

use orion_control_plane::{StatusEntry, StatusQuery, StatusSubject, TypedConfigValue};

use crate::cli::{OutputFormat, StatusArgs};
use crate::render::print_structured;

pub(super) async fn run(args: StatusArgs) -> Result<(), String> {
    if args.source.http.is_some() {
        return Err(
            "the status lane is local to each node and not served over HTTP; use --socket"
                .to_owned(),
        );
    }
    let subject = args
        .subject
        .as_deref()
        .map(str::parse::<StatusSubject>)
        .transpose()
        .map_err(|error| error.to_string())?;
    let query = StatusQuery {
        subject,
        key_prefix: args.key_prefix.clone(),
    };
    let entries = args
        .source
        .local_client()?
        .query_status(query)
        .await
        .map_err(|error| error.to_string())?;
    match args.source.output {
        OutputFormat::Summary => {
            print!("{}", render_status_summary(&entries, now_ms()));
            Ok(())
        }
        OutputFormat::Json | OutputFormat::Yaml | OutputFormat::Toml => {
            print_structured(&entries, args.source.output)
        }
        OutputFormat::Metrics => {
            Err("metrics output is supported only for observability views".to_owned())
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn render_status_summary(entries: &[StatusEntry], now_ms: u64) -> String {
    let mut out = format!("status count={}\n", entries.len());
    for entry in entries {
        out.push_str(&format!(
            "status subject={} key={} type={} value={} age_ms={} expires_in_ms={}\n",
            entry.subject,
            entry.key,
            entry.value.kind_name(),
            render_value(&entry.value),
            now_ms.saturating_sub(entry.published_at_ms),
            entry.expires_at_ms().saturating_sub(now_ms),
        ));
    }
    out
}

fn render_value(value: &TypedConfigValue) -> String {
    match value {
        TypedConfigValue::Bool(value) => value.to_string(),
        TypedConfigValue::Int(value) => value.to_string(),
        TypedConfigValue::UInt(value) => value.to_string(),
        TypedConfigValue::String(value)
            if value.is_empty() || value.contains(char::is_whitespace) =>
        {
            format!("{value:?}")
        }
        TypedConfigValue::String(value) => value.clone(),
        TypedConfigValue::Bytes(bytes) => {
            let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            format!("0x{hex}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_core::ProviderId;

    #[test]
    fn summary_lists_one_line_per_entry() {
        let mut entry = StatusEntry::new(
            StatusSubject::Provider(ProviderId::new("provider.camera")),
            "fps",
            TypedConfigValue::UInt(30),
        );
        entry.published_at_ms = 1_000;
        entry.ttl_ms = 5_000;
        let text = render_status_summary(&[entry], 1_500);
        assert_eq!(
            text,
            "status count=1\nstatus subject=provider/provider.camera key=fps type=uint value=30 \
             age_ms=500 expires_in_ms=4500\n"
        );
        assert_eq!(
            render_value(&TypedConfigValue::String("two words".into())),
            "\"two words\""
        );
        assert_eq!(
            render_value(&TypedConfigValue::Bytes(vec![1, 255])),
            "0x01ff"
        );
    }
}
