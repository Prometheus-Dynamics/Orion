use orion_check::{Check, Checked, Policy, Registered, Request, Security, What};

struct Ping;

impl Request for Ping {
    fn what(&self) -> What<'_> {
        What::Read("ping")
    }
}

fn handler(_request: Checked<Ping>) {}

fn main() {
    let policy: Policy = Default::default();
    let _registered = Registered::new("reboot", policy);
    let _ = (Check::new(Security::Off), handler);
}
