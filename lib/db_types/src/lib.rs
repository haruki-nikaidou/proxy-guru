//! Typed row ids and text-backed enums for the PostgreSQL entity layer.
//!
//! Every table's id is a distinct newtype over the bare key string
//! ([`table_record!`]), so a `NodeId` cannot be passed where a `ServerId` is
//! expected, in Rust or in a bind. Enums stored as text get their column
//! encoding from [`text_enum!`], which also fixes the one spelling the database,
//! the JSON documents and the wire share.
//!
//! No `Deref<Target = String>` on the ids, on purpose: an id is not a string
//! that happens to be typed, it is the identity of a row, and the few places
//! that need its text ask for it with [`as_str`](#) or `to_string()`.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]

const KEY_LEN: usize = 20;
const ALPHABET: &[u8; 36] = b"abcdefghijklmnopqrstuvwxyz0123456789";

/// A fresh 20-character `[a-z0-9]` key.
///
/// This is the shape the previous store minted, so rows imported from it and
/// rows created since are indistinguishable on the wire and in URLs. 36^20 is
/// about 2^103 possibilities; a collision on a table of a few thousand rows is
/// not a real event, and the primary key would refuse it anyway.
pub fn random_key() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    (0..KEY_LEN)
        .map(|_| {
            let index = rng.random_range(0..ALPHABET.len());
            char::from(ALPHABET[index])
        })
        .collect()
}

/// A text value that names no variant of a [`text_enum!`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown {type_name} value {value:?}")]
pub struct UnknownVariant {
    pub type_name: &'static str,
    pub value: String,
}

/// Define the id newtype of one table.
///
/// ```ignore
/// use db_types::table_record;
///
/// table_record!(CanvasId, "orchestration_canvas");
///
/// let id = CanvasId::new();                 // a fresh random key
/// let same = CanvasId::from_key(id.as_str()); // the key as it arrived on the wire
/// assert_eq!(id, same);
/// ```
///
/// The type is transparent to both sqlx and serde: it binds and decodes as
/// `text` (and `Vec<Id>` as `text[]`), and it serialises as a bare JSON string,
/// which is what makes ids inside `jsonb` documents typed as well.
///
/// The requesting crate must depend on `sqlx` and `serde`.
#[macro_export]
macro_rules! table_record {
    ($name:ident, $table:literal) => {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            ::serde::Serialize,
            ::serde::Deserialize,
            ::sqlx::Type,
        )]
        #[serde(transparent)]
        #[sqlx(transparent)]
        pub struct $name(pub ::std::string::String);

        impl $name {
            pub const TABLE: &'static str = $table;

            /// A fresh, random id for a row about to be created.
            ///
            /// Not `Default`: a random value is not a default value.
            #[allow(clippy::new_without_default)]
            pub fn new() -> Self {
                Self($crate::random_key())
            }

            /// The id of a row named by its bare key, as received on the wire.
            pub fn from_key(key: impl ::core::convert::Into<::std::string::String>) -> Self {
                Self(key.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> ::std::string::String {
                self.0
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl ::core::convert::AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

/// Give a fieldless enum its text spelling, once, for every purpose.
///
/// ```ignore
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// #[serde(rename_all = "snake_case")]
/// pub enum PortKind { DeriveListen, DeriveDestination, Bundle }
/// db_types::text_enum!(PortKind {
///     DeriveListen => "derive_listen",
///     DeriveDestination => "derive_destination",
///     Bundle => "bundle",
/// });
/// ```
///
/// Generates `as_str`, `FromStr`, `Display`, and the sqlx `Type`/`Encode`/
/// `Decode`/`PgHasArrayType` impls that make the enum a `text` column. Why not
/// `#[derive(sqlx::Type)]`: on a fieldless enum that derive maps to a PostgreSQL
/// enum *type* of the same name, and decoding a `text` column into it fails.
///
/// The serde spelling is declared separately on the enum; the module's tests
/// assert the two agree (see [`assert_text_enum_matches_serde!`]).
///
/// The requesting crate must depend on `sqlx`.
#[macro_export]
macro_rules! text_enum {
    ($name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        impl $name {
            /// Every variant, in declaration order.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                }
            }
        }

        impl ::core::str::FromStr for $name {
            type Err = $crate::UnknownVariant;

            fn from_str(s: &str) -> ::core::result::Result<Self, Self::Err> {
                match s {
                    $($text => ::core::result::Result::Ok(Self::$variant),)+
                    other => ::core::result::Result::Err($crate::UnknownVariant {
                        type_name: ::core::stringify!($name),
                        value: other.to_owned(),
                    }),
                }
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::sqlx::Type<::sqlx::Postgres> for $name {
            fn type_info() -> ::sqlx::postgres::PgTypeInfo {
                <str as ::sqlx::Type<::sqlx::Postgres>>::type_info()
            }

            fn compatible(ty: &::sqlx::postgres::PgTypeInfo) -> bool {
                <str as ::sqlx::Type<::sqlx::Postgres>>::compatible(ty)
            }
        }

        impl<'q> ::sqlx::Encode<'q, ::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut ::sqlx::postgres::PgArgumentBuffer,
            ) -> ::core::result::Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                <&str as ::sqlx::Encode<'q, ::sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
            }
        }

        impl<'r> ::sqlx::Decode<'r, ::sqlx::Postgres> for $name {
            fn decode(
                value: ::sqlx::postgres::PgValueRef<'r>,
            ) -> ::core::result::Result<Self, ::sqlx::error::BoxDynError> {
                let text = <&str as ::sqlx::Decode<'r, ::sqlx::Postgres>>::decode(value)?;
                ::core::result::Result::Ok(text.parse::<Self>()?)
            }
        }

        impl ::sqlx::postgres::PgHasArrayType for $name {
            fn array_type_info() -> ::sqlx::postgres::PgTypeInfo {
                <&str as ::sqlx::postgres::PgHasArrayType>::array_type_info()
            }
        }
    };
}

/// A test body asserting that a [`text_enum!`]'s spelling and its serde
/// spelling agree for every variant, so the column and the JSON documents can
/// never drift apart.
///
/// ```ignore
/// #[test]
/// fn port_kind_spellings_agree() {
///     db_types::assert_text_enum_matches_serde!(PortKind);
/// }
/// ```
///
/// The requesting crate must have `serde_json` available (a dev-dependency).
#[macro_export]
macro_rules! assert_text_enum_matches_serde {
    ($name:ident) => {
        for variant in <$name>::ALL {
            let json = ::serde_json::to_string(variant).unwrap_or_else(|e| {
                ::core::panic!("{} does not serialise: {e}", ::core::stringify!($name))
            });
            ::core::assert_eq!(
                json,
                ::std::format!("{:?}", variant.as_str()),
                "{}::{variant:?}: text_enum! says {:?}, serde says {json}",
                ::core::stringify!($name),
                variant.as_str()
            );
            let back: $name = json
                .trim_matches('"')
                .parse()
                .unwrap_or_else(|e| ::core::panic!("{e}"));
            ::core::assert_eq!(&back, variant);
        }
    };
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    table_record!(ThingId, "thing");

    #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Colour {
        Red,
        DeepBlue,
    }
    text_enum!(Colour { Red => "red", DeepBlue => "deep_blue" });

    #[test]
    fn a_key_has_the_shape_the_wire_expects() {
        for _ in 0..100 {
            let key = random_key();
            assert_eq!(key.len(), 20);
            assert!(
                key.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()),
                "{key}"
            );
        }
        assert_ne!(random_key(), random_key());
    }

    #[test]
    fn a_fresh_id_is_a_key_of_its_table() {
        let fresh = ThingId::new();
        assert_eq!(fresh.as_str().len(), 20);
        assert_eq!(fresh.clone().into_string(), fresh.0);
        assert_ne!(ThingId::new(), fresh);
    }

    #[test]
    fn an_id_is_transparent_to_serde() {
        let id = ThingId::from_key("zmfx8dqfhn9mm6pgo8id");
        assert_eq!(
            serde_json::to_string(&id).unwrap(),
            "\"zmfx8dqfhn9mm6pgo8id\""
        );
        let back: ThingId = serde_json::from_str("\"zmfx8dqfhn9mm6pgo8id\"").unwrap();
        assert_eq!(back, id);
        assert_eq!(id.to_string(), "zmfx8dqfhn9mm6pgo8id");
        assert_eq!(ThingId::TABLE, "thing");
    }

    #[test]
    fn a_text_enum_round_trips_and_names_the_unknown() {
        assert_eq!("deep_blue".parse::<Colour>().unwrap(), Colour::DeepBlue);
        assert_eq!(Colour::Red.to_string(), "red");
        let err = "mauve".parse::<Colour>().unwrap_err();
        assert_eq!(
            err,
            UnknownVariant {
                type_name: "Colour",
                value: "mauve".into()
            }
        );
        assert_text_enum_matches_serde!(Colour);
    }
}
