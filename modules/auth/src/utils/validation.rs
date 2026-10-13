//! Validation of auth request inputs with `wakuwaku::proof`.
//!
//! Every check here is pure, so each is a [`PureVerifier`]. The evidence types
//! carry a private field: a [`Proven`] of them can only be built by the verifiers
//! in this module. The rpc edge proves inputs before calling a service, and the
//! services and queries that store an email, a password or an API-key name take
//! the [`Proven`] value, so nothing unvalidated reaches the database.
//!
//! `Login` is deliberately not validated (only normalized): a policy check there
//! would leak the policy and lock out accounts created before it.

// `PureVerifier<String, _>` takes `fn(&String) -> _`, so the checks cannot take `&str`.
#![allow(clippy::ptr_arg)]

use std::convert::Infallible;

use wakuwaku::proof::{DeniedReason, Proven, PureVerifier, ValidateError};

/// The canonical form of an email for storage and lookup: trimmed and lowercased,
/// so the UNIQUE index and every query agree.
pub fn normalize_email(raw: &str) -> String {
    raw.trim().to_lowercase()
}

pub static EMAIL_NOT_CANONICAL: DeniedReason = DeniedReason("email address is not normalized");
pub static EMAIL_MALFORMED: DeniedReason = DeniedReason("not a valid email address");

/// Evidence that a string is a normalized, plausibly well-formed email address.
#[derive(Debug)]
pub struct CanonicalEmail(());

fn check_email(subject: &String) -> Result<CanonicalEmail, ValidateError<Infallible>> {
    if *subject != normalize_email(subject) {
        return Err(ValidateError::Deny(EMAIL_NOT_CANONICAL));
    }
    let well_formed = (3..=254).contains(&subject.len())
        && !subject
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
        && match subject.split_once('@') {
            Some((local, domain)) => {
                !local.is_empty() && !domain.is_empty() && !domain.contains('@')
            }
            None => false,
        };
    if !well_formed {
        return Err(ValidateError::Deny(EMAIL_MALFORMED));
    }
    Ok(CanonicalEmail(()))
}

pub static EMAIL: PureVerifier<String, CanonicalEmail> = PureVerifier::new(check_email);

/// Normalize `raw`, then prove it is a valid email address.
pub fn prove_email(
    raw: &str,
) -> Result<Proven<String, CanonicalEmail>, ValidateError<Infallible>> {
    EMAIL.prove(normalize_email(raw))
}

pub static PASSWORD_TOO_SHORT: DeniedReason =
    DeniedReason("password must be at least 8 characters");
pub static PASSWORD_TOO_LONG: DeniedReason = DeniedReason("password must be at most 1024 bytes");

/// Evidence that a string is acceptable as a new password.
#[derive(Debug)]
pub struct AcceptablePassword(());

fn check_password(subject: &String) -> Result<AcceptablePassword, ValidateError<Infallible>> {
    if subject.chars().count() < 8 {
        return Err(ValidateError::Deny(PASSWORD_TOO_SHORT));
    }
    if subject.len() > 1024 {
        return Err(ValidateError::Deny(PASSWORD_TOO_LONG));
    }
    Ok(AcceptablePassword(()))
}

pub static NEW_PASSWORD: PureVerifier<String, AcceptablePassword> =
    PureVerifier::new(check_password);

pub static API_KEY_NAME_EMPTY: DeniedReason = DeniedReason("API key name is empty");
pub static API_KEY_NAME_TOO_LONG: DeniedReason =
    DeniedReason("API key name must be at most 128 characters");
pub static API_KEY_NAME_CONTROL: DeniedReason =
    DeniedReason("API key name contains control characters");

/// Evidence that a string is acceptable as an API-key name.
#[derive(Debug)]
pub struct ApiKeyName(());

fn check_api_key_name(subject: &String) -> Result<ApiKeyName, ValidateError<Infallible>> {
    if subject.is_empty() || subject.trim() != subject {
        return Err(ValidateError::Deny(API_KEY_NAME_EMPTY));
    }
    if subject.chars().count() > 128 {
        return Err(ValidateError::Deny(API_KEY_NAME_TOO_LONG));
    }
    if subject.chars().any(char::is_control) {
        return Err(ValidateError::Deny(API_KEY_NAME_CONTROL));
    }
    Ok(ApiKeyName(()))
}

pub static API_KEY_NAME: PureVerifier<String, ApiKeyName> = PureVerifier::new(check_api_key_name);

#[cfg(test)]
mod tests {
    use super::*;

    fn denied<P>(result: Result<Proven<String, P>, ValidateError<Infallible>>) -> DeniedReason {
        match result {
            Ok(_) => DeniedReason("accepted"),
            Err(ValidateError::Deny(reason)) => reason,
            Err(ValidateError::IoError(never)) => match never {},
        }
    }

    #[test]
    fn email_is_normalized_and_checked() {
        assert!(matches!(
            prove_email(" Admin@Example.com "),
            Ok(p) if p.subject() == "admin@example.com"
        ));
        assert!(denied(EMAIL.prove("Admin@example.com".into())) == EMAIL_NOT_CANONICAL);
        assert!(denied(EMAIL.prove(" a@b".into())) == EMAIL_NOT_CANONICAL);
        assert!(denied(prove_email("nope")) == EMAIL_MALFORMED);
        assert!(denied(prove_email("a@")) == EMAIL_MALFORMED);
        assert!(denied(prove_email("@b")) == EMAIL_MALFORMED);
        assert!(denied(prove_email("a@b@c")) == EMAIL_MALFORMED);
        assert!(denied(prove_email("a b@c")) == EMAIL_MALFORMED);
        assert!(denied(prove_email("a\u{7}@c")) == EMAIL_MALFORMED);
        assert!(denied(prove_email(&format!("a@{}", "b".repeat(253)))) == EMAIL_MALFORMED);
    }

    #[test]
    fn new_password_bounds() {
        assert!(NEW_PASSWORD.prove("long-enough".into()).is_ok());
        assert!(denied(NEW_PASSWORD.prove("short".into())) == PASSWORD_TOO_SHORT);
        assert!(denied(NEW_PASSWORD.prove("x".repeat(1025))) == PASSWORD_TOO_LONG);
    }

    #[test]
    fn api_key_name_rules() {
        assert!(API_KEY_NAME.prove("ci deploy".into()).is_ok());
        assert!(denied(API_KEY_NAME.prove(String::new())) == API_KEY_NAME_EMPTY);
        assert!(denied(API_KEY_NAME.prove(" padded".into())) == API_KEY_NAME_EMPTY);
        assert!(denied(API_KEY_NAME.prove("x".repeat(129))) == API_KEY_NAME_TOO_LONG);
        assert!(denied(API_KEY_NAME.prove("tab\u{1}name".into())) == API_KEY_NAME_CONTROL);
    }
}
