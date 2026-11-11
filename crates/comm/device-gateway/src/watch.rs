//! A service, watched for what devices do through it.

use orion_msg_actions::{Register, Target};
use orion_server::{
    Checked, Cx, Error, Incoming, Policy, Roles, Service, SessionId, Sessions, What,
};
use orion_wire::{Decode, FrameView, body, kind};

use crate::{DeviceGateway, Offer};

/// `S`, unchanged, except that a topic a device declared or an action it registered through
/// it is noted with the gateway once `S` took it.
#[derive(Debug)]
pub struct Watch<S> {
    inner: S,
    gateway: DeviceGateway,
}

impl<S> Watch<S> {
    pub(crate) const fn new(inner: S, gateway: DeviceGateway) -> Self {
        Self { inner, gateway }
    }
}

/// What a frame offers, if it offers anything.
fn offer(frame: &FrameView<'_>) -> Option<Offer> {
    match frame.kind() {
        kind::TOPIC_DECLARE => {
            let (topic, _) = body::take::<&str>(frame.payload()).ok()?;
            Some(Offer::Topic(topic.to_owned()))
        }
        kind::ACTION_REGISTER => {
            let register = Register::decode(frame.payload()).ok()?;
            let full = match &register.target {
                Target::Resource(resource) => {
                    format!("{}.{}", resource.as_str(), register.name.as_str())
                }
                Target::Node(_) => register.name.as_str().to_owned(),
            };
            Some(Offer::Action(full))
        }
        _ => None,
    }
}

impl<S: Service> Service for Watch<S> {
    fn kinds(&self) -> &'static [u8] {
        self.inner.kinds()
    }

    fn asks<'a>(&self, frame: &FrameView<'a>) -> Result<(What<'a>, Policy), Error> {
        self.inner.asks(frame)
    }

    fn handle(&mut self, request: Checked<Incoming<'_>>, cx: &mut Cx<'_>) -> Result<(), Error> {
        let caller = cx.caller();
        let noted = caller
            .roles
            .has(Roles::DEVICE)
            .then(|| offer(request.get().frame()).map(|offer| (caller.name.clone(), offer)))
            .flatten();
        let from = cx.from();
        self.inner.handle(request, cx)?;
        if let Some((name, offer)) = noted {
            self.gateway.offered(from, &name, offer);
        }
        Ok(())
    }

    fn closed(&mut self, session: SessionId, sessions: &mut Sessions) {
        self.inner.closed(session, sessions);
        self.gateway.gone(session);
    }
}
