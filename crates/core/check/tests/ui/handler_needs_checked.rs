use orion_check::{Check, Checked, Policy, Registered, Request, Security, What};

struct Ping;

impl Request for Ping {
    fn what(&self) -> What<'_> {
        What::Read("ping")
    }
}

fn handler(_request: Checked<Ping>) {}

fn main() {
    handler(Ping);
    let _ = (Check::new(Security::Off), None::<Policy>, None::<Registered<u8>>);
}
