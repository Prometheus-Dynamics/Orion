pub(super) fn gauge<T>(out: &mut String, name: &str, help: &str, labels: &[(&str, &str)], value: T)
where
    T: std::fmt::Display,
{
    metric_help(out, name, help);
    metric_type(out, name, "gauge");
    sample(out, name, labels, value);
}

pub(super) fn optional_gauge<T>(
    out: &mut String,
    name: &str,
    help: &str,
    labels: &[(&str, &str)],
    value: Option<T>,
) where
    T: std::fmt::Display,
{
    if let Some(value) = value {
        gauge(out, name, help, labels, value);
    }
}

pub(super) fn metric_help(out: &mut String, name: &str, help: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
}

pub(super) fn metric_type(out: &mut String, name: &str, metric_type: &str) {
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(metric_type);
    out.push('\n');
}

pub(super) fn sample<T>(out: &mut String, name: &str, labels: &[(&str, &str)], value: T)
where
    T: std::fmt::Display,
{
    out.push_str(name);
    append_labels(out, labels.iter().copied());
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

pub(super) fn sample_owned<T>(out: &mut String, name: &str, labels: &[(String, String)], value: T)
where
    T: std::fmt::Display,
{
    out.push_str(name);
    append_labels(
        out,
        labels
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

pub(super) fn append_labels<'a>(
    out: &mut String,
    labels: impl Iterator<Item = (&'a str, &'a str)>,
) {
    let labels = labels.collect::<Vec<_>>();
    if labels.is_empty() {
        return;
    }
    out.push('{');
    for (index, (key, value)) in labels.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(key);
        out.push_str("=\"");
        append_escaped_label_value(out, value);
        out.push('"');
    }
    out.push('}');
}

pub(super) fn append_escaped_label_value(out: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
}

pub(super) fn milli_to_f64(value: u64) -> f64 {
    value as f64 / 1000.0
}
