//! Rendering a timestamp for the API.

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// RFC 3339, the one shape a timestamp crosses the API in.
///
/// Formatting a value that came from the database cannot fail; an empty string
/// is what an absent timestamp already sends, so a failure degrades to that
/// rather than to a panic on the request path.
pub fn rfc3339(at: OffsetDateTime) -> String {
    at.format(&Rfc3339).unwrap_or_default()
}
