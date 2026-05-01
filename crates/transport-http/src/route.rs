#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
}

impl HttpMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }

    pub fn from_http_name(value: &str) -> Option<Self> {
        match value {
            "GET" => Some(Self::Get),
            "POST" => Some(Self::Post),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlRoute {
    Hello,
    Sync,
    Snapshot,
    Mutations,
    Observability,
    Health,
    Readiness,
    ObservedUpdate,
}

impl ControlRoute {
    pub fn path(self) -> &'static str {
        match self {
            Self::Hello => "/v1/control/hello",
            Self::Sync => "/v1/control/sync",
            Self::Snapshot => "/v1/control/snapshot",
            Self::Mutations => "/v1/control/mutations",
            Self::Observability => "/v1/control/observability",
            Self::Health => "/v1/control/health",
            Self::Readiness => "/v1/control/readiness",
            Self::ObservedUpdate => "/v1/control/observed",
        }
    }

    pub fn method(self) -> HttpMethod {
        match self {
            Self::Hello
            | Self::Sync
            | Self::Snapshot
            | Self::Mutations
            | Self::Observability
            | Self::ObservedUpdate => HttpMethod::Post,
            Self::Health | Self::Readiness => HttpMethod::Get,
        }
    }

    pub fn parse(path: &str, method: HttpMethod) -> Option<Self> {
        let route = match path {
            "/v1/control/hello" => Self::Hello,
            "/v1/control/sync" => Self::Sync,
            "/v1/control/snapshot" => Self::Snapshot,
            "/v1/control/mutations" => Self::Mutations,
            "/v1/control/observability" => Self::Observability,
            "/v1/control/health" => Self::Health,
            "/v1/control/readiness" => Self::Readiness,
            "/v1/control/observed" => Self::ObservedUpdate,
            _ => return None,
        };

        if route.method() == method {
            Some(route)
        } else {
            None
        }
    }
}
