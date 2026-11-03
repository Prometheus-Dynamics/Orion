//! Actions for link devices (`docs/actions.md`, "Link devices"; `docs/link-protocol.md`,
//! "Actions").
//!
//! An action whose target is a provider a link device publishes (or a resource of that provider)
//! is routed to the device's link task, which sends it as an `ActionRequest` (one action in flight
//! per device; the device refuses others as busy). The device's `ActionResult`s come back to the
//! node through [`NodeApp::report_link_action`], and a device that disconnects (or a link that
//! closes) fails the actions still in flight on it.

use super::{ProviderOwner, lock};
use crate::NodeApp;
use crate::app::actions::registry::ActionRoute;
use orion::{
    ProviderId,
    control_plane::{ActionRequest, ActionState, TypedConfigValue},
};
use std::collections::BTreeMap;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

/// A request for a link task to send to one of its devices.
pub(crate) struct LinkActionCommand {
    pub(crate) device: String,
    pub(crate) action_id: String,
    pub(crate) request: ActionRequest,
}

impl NodeApp {
    /// Registers the command channel of the link task `link` (replacing an older one) and returns
    /// its receiving end.
    pub(crate) fn link_actions_open(&self, link: &str) -> UnboundedReceiver<LinkActionCommand> {
        let (sender, receiver) = unbounded_channel();
        lock(&self.state.links.action_senders).insert(link.to_owned(), sender);
        receiver
    }

    /// Forgets the command channel of `link` (its task has stopped).
    pub(crate) fn link_actions_closed(&self, link: &str) {
        lock(&self.state.links.action_senders).remove(link);
    }

    /// The route of `provider` when a connected link device publishes it.
    pub(crate) fn link_action_route(&self, provider: &ProviderId) -> Option<ActionRoute> {
        let owners = lock(&self.state.links.owners);
        let ProviderOwner::Device(claim) = owners.get(provider)? else {
            return None;
        };
        if !claim.connected {
            return None;
        }
        lock(&self.state.links.action_senders)
            .contains_key(&claim.link)
            .then(|| ActionRoute::Link {
                link: claim.link.clone(),
                device: claim.device.clone(),
            })
    }

    /// Hands a request to the link task that serves `device`.
    pub(crate) fn deliver_link_action(
        &self,
        link: &str,
        device: &str,
        action_id: &str,
        request: ActionRequest,
    ) -> Result<(), String> {
        let senders = lock(&self.state.links.action_senders);
        let sender = senders
            .get(link)
            .ok_or_else(|| format!("link {link} is not running"))?;
        sender
            .send(LinkActionCommand {
                device: device.to_owned(),
                action_id: action_id.to_owned(),
                request,
            })
            .map_err(|_| format!("link {link} stopped before the action was sent"))
    }

    /// Records a report from a link device about an action sent to it. Ignored unless the action
    /// was routed to that device (and is not final yet).
    pub(crate) fn report_link_action(
        &self,
        action_id: &str,
        link: &str,
        device: &str,
        state: ActionState,
        output: BTreeMap<String, TypedConfigValue>,
    ) {
        let expected = ActionRoute::Link {
            link: link.to_owned(),
            device: device.to_owned(),
        };
        let routed = self
            .action_registry()
            .get(action_id)
            .is_some_and(|entry| entry.route == expected);
        if routed {
            self.update_node_action(action_id, state, output);
        }
    }

    /// Fails the actions in flight on `device` of `link` (all devices of the link when `None`).
    pub(crate) fn fail_link_actions(&self, link: &str, device: Option<&str>, reason: &str) {
        let ids = self.action_registry().running_for_link(link, device);
        for action_id in ids {
            self.finish_action(
                &action_id,
                ActionState::Failed {
                    reason: reason.to_owned(),
                },
            );
        }
    }
}
