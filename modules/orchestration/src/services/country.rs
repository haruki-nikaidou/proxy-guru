//! The country of each server's IPv4 address, looked up by the master.
//!
//! The dashboard draws a server's flag from it. A pass asks one HTTP service
//! (`country_lookup_url`) about every distinct address that has no answer yet —
//! the IPv4 [`ServerEntity::v4_address`] picks — so an address is asked about
//! once, again when the server's address changes, and after a failure only once
//! `country_lookup_retry_after` has passed. Workers look nothing up: a fleet of
//! them asking a free service every minute is what ran into its rate limit.

use crate::config::OrchestrationConfig;
use crate::entities::db::server::{ListAllServers, ServerEntity, SetServerCountry};
use crate::services::OrchestrationError;
use base::db::Db;
use kanau::processor::Processor;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::time::Duration;
use time::OffsetDateTime;

/// Per-request budget: a service that does not answer within this failed.
pub const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct CountryService {
    pub db: Db,
    pub config: OrchestrationConfig,
    pub http: reqwest::Client,
}

/// One pass over every server of every canvas.
pub struct ResolveServerCountries {
    pub now: OffsetDateTime,
}

/// What one pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct CountryPass {
    /// Distinct addresses the service was asked about.
    pub looked_up: usize,
    /// Of those, the ones it answered with a country.
    pub resolved: usize,
    /// Servers whose stored lookup was dropped: they have no public IPv4 now.
    pub cleared: usize,
}

/// What a pass knows about one server's country.
#[derive(Debug, Clone, Copy)]
pub struct CountryState<'a> {
    /// The address a lookup would be for ([`ServerEntity::v4_address`]).
    pub v4: Option<Ipv4Addr>,
    pub country: Option<&'a str>,
    /// The address the stored lookup was made for.
    pub address: Option<&'a str>,
    pub checked_at: Option<OffsetDateTime>,
}

impl<'a> CountryState<'a> {
    pub fn of(server: &'a ServerEntity) -> Self {
        Self {
            v4: server.v4_address(),
            country: server.country.as_deref(),
            address: server.country_address.as_deref(),
            checked_at: server.country_checked_at,
        }
    }
}

/// What a pass does with one server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountryAction {
    /// The stored answer is for the current address, or a failure too recent to
    /// retry.
    Skip,
    /// The server has no public IPv4 any more: forget the stored lookup.
    Clear,
    Lookup(Ipv4Addr),
}

/// Decides the action for one server. A new or changed address is looked up at
/// once; a failed lookup of the same address is retried after `retry_after`.
pub fn country_action(
    state: CountryState<'_>,
    now: OffsetDateTime,
    retry_after: Duration,
) -> CountryAction {
    let Some(v4) = state.v4.filter(|v4| is_global_v4(*v4)) else {
        return if state.address.is_some() {
            CountryAction::Clear
        } else {
            CountryAction::Skip
        };
    };
    if state.address != Some(v4.to_string().as_str()) {
        return CountryAction::Lookup(v4);
    }
    if state.country.is_some() {
        return CountryAction::Skip;
    }
    // A retry window too large to add to the calendar never comes due.
    let due = state.checked_at.is_none_or(|checked| {
        time::Duration::try_from(retry_after)
            .ok()
            .and_then(|retry_after| checked.checked_add(retry_after))
            .is_some_and(|retry_at| now >= retry_at)
    });
    if due {
        CountryAction::Lookup(v4)
    } else {
        CountryAction::Skip
    }
}

/// Whether a lookup service can know where `address` is. Private, loopback,
/// link-local, shared (100.64.0.0/10), documentation, benchmarking (198.18.0.0/15),
/// multicast, reserved and unspecified addresses are nowhere.
pub fn is_global_v4(address: Ipv4Addr) -> bool {
    let [first, second, ..] = address.octets();
    !(address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_unspecified()
        || address.is_documentation()
        || address.is_multicast()
        || first == 0
        || first >= 240
        || (first == 100 && (second & 0xc0) == 64)
        || (first == 198 && (second & 0xfe) == 18))
}

/// The lookup URL for `address`: every `{ip}` of the template replaced.
pub fn lookup_url(template: &str, address: Ipv4Addr) -> String {
    template.trim().replace("{ip}", &address.to_string())
}

/// The two-letter code in a lookup service's answer, upper-cased: the `country`
/// field of a JSON object, or else the whole body. An error page, a JSON error or
/// the empty body of an address the service does not know is `None`.
pub fn parse_country(body: &str) -> Option<String> {
    let body = body.trim();
    let code = if body.starts_with('{') {
        let value: serde_json::Value = serde_json::from_str(body).ok()?;
        value.get("country")?.as_str()?.trim().to_ascii_uppercase()
    } else {
        body.to_ascii_uppercase()
    };
    (code.len() == 2 && code.bytes().all(|b| b.is_ascii_uppercase())).then_some(code)
}

impl CountryService {
    /// Asks the service about one address. Every failure is a `None` and a
    /// warning: the answer is cosmetic, and the address is asked about again
    /// after `country_lookup_retry_after`.
    async fn look_up(&self, template: &str, address: Ipv4Addr) -> Option<String> {
        let url = lookup_url(template, address);
        let answer = match self
            .http
            .get(&url)
            .timeout(LOOKUP_TIMEOUT)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
        {
            Ok(response) => response.text().await,
            Err(error) => Err(error),
        };
        match answer {
            Ok(body) => {
                let country = parse_country(&body);
                if country.is_none() {
                    tracing::warn!(%address, url = %url, "country lookup answered without a country");
                }
                country
            }
            Err(error) => {
                tracing::warn!(%address, url = %url, error = %error, "country lookup failed");
                None
            }
        }
    }
}

impl Processor<ResolveServerCountries> for CountryService {
    type Output = CountryPass;
    type Error = OrchestrationError;
    #[tracing::instrument(name = "Service:ResolveServerCountries", skip_all, err)]
    async fn process(&self, input: ResolveServerCountries) -> Result<Self::Output, Self::Error> {
        let template = self.config.country_lookup_url.trim();
        if template.is_empty() {
            return Ok(CountryPass::default());
        }
        let retry_after = self.config.country_lookup_retry_after();
        let mut pass = CountryPass::default();
        // Servers sharing an address share its answer, asked for once per pass.
        let mut answers: HashMap<Ipv4Addr, Option<String>> = HashMap::new();
        for server in self.db.process(ListAllServers).await? {
            match country_action(CountryState::of(&server), input.now, retry_after) {
                CountryAction::Skip => {}
                CountryAction::Clear => {
                    self.db
                        .process(SetServerCountry {
                            server: server.id,
                            address: None,
                            country: None,
                            checked_at: None,
                        })
                        .await?;
                    pass.cleared = pass.cleared.saturating_add(1);
                }
                CountryAction::Lookup(address) => {
                    let country = match answers.get(&address) {
                        Some(answer) => answer.clone(),
                        None => {
                            let answer = self.look_up(template, address).await;
                            pass.looked_up = pass.looked_up.saturating_add(1);
                            pass.resolved =
                                pass.resolved.saturating_add(usize::from(answer.is_some()));
                            answers.insert(address, answer.clone());
                            answer
                        }
                    };
                    self.db
                        .process(SetServerCountry {
                            server: server.id,
                            address: Some(address.to_string()),
                            country,
                            checked_at: Some(input.now),
                        })
                        .await?;
                }
            }
        }
        Ok(pass)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: Duration = Duration::from_secs(3600);

    fn at(secs: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(secs).unwrap_or(OffsetDateTime::UNIX_EPOCH)
    }

    fn state<'a>(
        v4: Option<&str>,
        country: Option<&'a str>,
        address: Option<&'a str>,
        checked_at: Option<OffsetDateTime>,
    ) -> CountryState<'a> {
        CountryState {
            v4: v4.and_then(|v4| v4.parse().ok()),
            country,
            address,
            checked_at,
        }
    }

    fn v4(address: &str) -> Ipv4Addr {
        address.parse().unwrap_or(Ipv4Addr::UNSPECIFIED)
    }

    #[test]
    fn a_new_or_changed_address_is_looked_up_at_once() {
        let now = at(10_000);
        let never = state(Some("8.8.8.8"), None, None, None);
        assert_eq!(
            country_action(never, now, HOUR),
            CountryAction::Lookup(v4("8.8.8.8"))
        );
        let moved = state(Some("1.1.1.1"), Some("US"), Some("8.8.8.8"), Some(now));
        assert_eq!(
            country_action(moved, now, HOUR),
            CountryAction::Lookup(v4("1.1.1.1"))
        );
    }

    #[test]
    fn a_known_answer_is_kept() {
        let now = at(10_000);
        let known = state(Some("8.8.8.8"), Some("US"), Some("8.8.8.8"), Some(at(0)));
        assert_eq!(country_action(known, now, HOUR), CountryAction::Skip);
    }

    #[test]
    fn a_failed_lookup_waits_for_the_retry_delay() {
        let failed = state(Some("8.8.8.8"), None, Some("8.8.8.8"), Some(at(0)));
        assert_eq!(country_action(failed, at(3599), HOUR), CountryAction::Skip);
        assert_eq!(
            country_action(failed, at(3600), HOUR),
            CountryAction::Lookup(v4("8.8.8.8"))
        );
    }

    #[test]
    fn a_server_without_a_public_ipv4_is_never_looked_up() {
        let now = at(10_000);
        assert_eq!(
            country_action(state(None, None, None, None), now, HOUR),
            CountryAction::Skip
        );
        assert_eq!(
            country_action(state(Some("10.0.0.5"), None, None, None), now, HOUR),
            CountryAction::Skip
        );
        let lost = state(Some("10.0.0.5"), Some("US"), Some("8.8.8.8"), Some(at(0)));
        assert_eq!(country_action(lost, now, HOUR), CountryAction::Clear);
    }

    #[test]
    fn only_global_addresses_have_a_country() {
        for global in ["8.8.8.8", "185.14.47.132", "100.128.0.1", "198.20.0.1"] {
            assert!(is_global_v4(v4(global)), "{global}");
        }
        for local in [
            "0.0.0.0",
            "0.1.2.3",
            "10.0.0.1",
            "100.64.0.1",
            "100.127.255.254",
            "127.0.0.1",
            "169.254.1.1",
            "172.16.0.1",
            "192.0.2.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.19.255.254",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
        ] {
            assert!(!is_global_v4(v4(local)), "{local}");
        }
    }

    #[test]
    fn the_template_takes_the_address() {
        assert_eq!(
            lookup_url(" https://api.country.is/{ip} ", v4("8.8.8.8")),
            "https://api.country.is/8.8.8.8"
        );
        assert_eq!(
            lookup_url(
                "https://get.geojs.io/v1/ip/country/{ip}.json",
                v4("1.1.1.1")
            ),
            "https://get.geojs.io/v1/ip/country/1.1.1.1.json"
        );
    }

    #[test]
    fn answers_are_read_as_json_or_as_bare_codes() {
        assert_eq!(
            parse_country(r#"{"ip":"185.14.47.132","country":"HK"}"#).as_deref(),
            Some("HK")
        );
        assert_eq!(parse_country("HK\n").as_deref(), Some("HK"));
        assert_eq!(parse_country(" cn ").as_deref(), Some("CN"));
        for junk in [
            "",
            "\n",
            "USA",
            "H1",
            "<html>429</html>",
            r#"{"error":{"code":404,"message":"Not Found"}}"#,
            r#"{"country":12}"#,
            r#"{"country":"United States"}"#,
            "{not json",
        ] {
            assert_eq!(parse_country(junk), None, "{junk}");
        }
    }
}
