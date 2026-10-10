//! Text frames: JSON arrays of control messages `{"method": ..., "params": ...}`.

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// A topic's properties, as a JSON object (`persistent`, `retained`, ... and any extra keys).
pub type Properties = serde_json::Map<String, serde_json::Value>;

/// A control message in either direction. Client-to-server methods are `publish`, `unpublish`,
/// `setproperties`, `subscribe` and `unsubscribe`; server-to-client methods are `announce`,
/// `unannounce` and `properties`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params", rename_all = "lowercase")]
pub enum Control {
    /// Client: start publishing `name` with `pubuid`.
    Publish(PublishParams),
    /// Client: stop publishing `pubuid`.
    Unpublish(UnpublishParams),
    /// Client: merge `update` into a topic's properties (a `null` value removes the key).
    Setproperties(SetPropertiesParams),
    /// Client: subscribe `subuid` to topics (names, or prefixes when `options.prefix`).
    Subscribe(SubscribeParams),
    /// Client: drop subscription `subuid`.
    Unsubscribe(UnsubscribeParams),
    /// Server: a topic exists (or came back), with its id, type and properties.
    Announce(AnnounceParams),
    /// Server: a topic is gone for this client.
    Unannounce(UnannounceParams),
    /// Server: a topic's properties changed.
    Properties(PropertiesParams),
}

/// Parameters of `publish`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PublishParams {
    /// Topic name, for example `/SmartDashboard/Speed`.
    pub name: String,
    /// NT4 type string, serialized as `type`.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Publisher uid chosen by the client; binary frames use it as their topic id.
    pub pubuid: u32,
    /// Initial properties.
    #[serde(default, skip_serializing_if = "Properties::is_empty")]
    pub properties: Properties,
}

/// Parameters of `unpublish`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnpublishParams {
    /// The publisher uid from `publish`.
    pub pubuid: u32,
}

/// Parameters of `setproperties`, and of the server's `properties`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SetPropertiesParams {
    /// Topic name.
    pub name: String,
    /// Keys to set; `null` removes a key.
    #[serde(default)]
    pub update: Properties,
}

/// Parameters of the server's `properties`.
pub type PropertiesParams = SetPropertiesParams;

/// Options of `subscribe`. Missing fields take the NT4 defaults.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SubscribeOptions {
    /// Seconds between value updates the client wants (a hint; the server may send faster).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub periodic: Option<f64>,
    /// Also send the client's own publications back to it.
    #[serde(default)]
    pub all: bool,
    /// Announce topics but send no values.
    #[serde(default, rename = "topicsonly")]
    pub topics_only: bool,
    /// Treat each subscribed name as a prefix.
    #[serde(default)]
    pub prefix: bool,
}

/// Parameters of `subscribe`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubscribeParams {
    /// Topic names or prefixes.
    pub topics: Vec<String>,
    /// Subscriber uid chosen by the client.
    pub subuid: u32,
    /// Options.
    #[serde(default)]
    pub options: SubscribeOptions,
}

/// Parameters of `unsubscribe`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnsubscribeParams {
    /// The subscriber uid from `subscribe`.
    pub subuid: u32,
}

/// Parameters of `announce`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AnnounceParams {
    /// Topic name.
    pub name: String,
    /// Server-assigned topic id, used by binary frames.
    pub id: i32,
    /// NT4 type string, serialized as `type`.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Set on the announce that goes back to the client which publishes the topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pubuid: Option<u32>,
    /// The topic's properties.
    #[serde(default)]
    pub properties: Properties,
}

/// Parameters of `unannounce`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnannounceParams {
    /// Topic name.
    pub name: String,
    /// The topic id the client knew it by.
    pub id: i32,
}

/// Whether `name` is selected by a subscription's topic list: an exact name, or a prefix when
/// `prefix` is set.
pub(crate) fn name_matches(topics: &[String], prefix: bool, name: &str) -> bool {
    topics.iter().any(|t| {
        if prefix {
            name.starts_with(t.as_str())
        } else {
            name == t
        }
    })
}

/// Parses a text frame into control messages. Elements with an unknown method are skipped and
/// returned as `Err` strings, so one unfamiliar message does not drop the rest of the frame.
pub fn parse_text(text: &str) -> Result<Vec<std::result::Result<Control, String>>> {
    let items: Vec<serde_json::Value> = serde_json::from_str(text)?;
    Ok(items
        .into_iter()
        .map(|item| {
            let method = item
                .get("method")
                .and_then(|m| m.as_str())
                .unwrap_or("")
                .to_owned();
            serde_json::from_value::<Control>(item).map_err(|e| format!("{method}: {e}"))
        })
        .collect())
}

/// Serializes control messages as one text frame (a JSON array).
pub fn encode_text(messages: &[Control]) -> Result<String> {
    Ok(serde_json::to_string(messages)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_uses_spec_field_names() {
        let msg = Control::Subscribe(SubscribeParams {
            topics: vec!["/a/".into()],
            subuid: 3,
            options: SubscribeOptions {
                prefix: true,
                topics_only: true,
                ..Default::default()
            },
        });
        let json = encode_text(&[msg]).unwrap();
        assert_eq!(
            json,
            r#"[{"method":"subscribe","params":{"topics":["/a/"],"subuid":3,"options":{"all":false,"topicsonly":true,"prefix":true}}}]"#
        );
    }

    #[test]
    fn announce_parses_and_unknown_methods_are_skipped() {
        let text = r#"[
            {"method":"announce","params":{"name":"/t","id":4,"type":"double","pubuid":9,"properties":{"retained":true}}},
            {"method":"mystery","params":{}},
            {"method":"unannounce","params":{"name":"/t","id":4}}
        ]"#;
        let parsed = parse_text(text).unwrap();
        assert_eq!(parsed.len(), 3);
        match &parsed[0] {
            Ok(Control::Announce(a)) => {
                assert_eq!(a.id, 4);
                assert_eq!(a.type_name, "double");
                assert_eq!(a.pubuid, Some(9));
                assert_eq!(
                    a.properties.get("retained").and_then(|v| v.as_bool()),
                    Some(true)
                );
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(parsed[1].is_err());
        assert!(matches!(parsed[2], Ok(Control::Unannounce(_))));
    }

    #[test]
    fn subscribe_defaults_when_options_are_missing() {
        let parsed =
            parse_text(r#"[{"method":"subscribe","params":{"topics":["/x"],"subuid":1}}]"#)
                .unwrap();
        match &parsed[0] {
            Ok(Control::Subscribe(s)) => {
                assert!(!s.options.prefix && !s.options.all && !s.options.topics_only);
                assert_eq!(s.options.periodic, None);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
