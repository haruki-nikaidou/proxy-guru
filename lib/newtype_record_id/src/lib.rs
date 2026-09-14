//! Reusable helpers for SurrealDB record-id newtypes.
//!
//! A record-id newtype wraps a [`surrealdb::types::RecordId`] that is pinned to
//! a single table, giving each table a distinct, type-checked id type. The
//! boilerplate `SurrealValue` implementation is identical for every such type,
//! so [`table_record!`] generates it from a struct name and a table name.

/// Define a `RecordId` newtype bound to a single table.
///
/// ```ignore
/// use newtype_record_id::table_record;
///
/// table_record!(CanvasId, "canvas");
/// ```
///
/// This expands to a `pub struct CanvasId(pub RecordId)` plus a
/// [`surrealdb::types::SurrealValue`] implementation whose `kind_of`,
/// `is_value`, and `from_value` are all constrained to the `"canvas"` table.
///
/// The requesting crate must depend on `surrealdb`.
#[macro_export]
macro_rules! table_record {
    ($name:ident, $table:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name(pub ::surrealdb::types::RecordId);

        impl $name {
            const TABLE: &'static str = $table;
        }

        impl ::surrealdb::types::SurrealValue for $name {
            fn kind_of() -> ::surrealdb::types::Kind {
                ::surrealdb::types::Kind::Record(::std::vec![
                    ::surrealdb::types::Table::new(<$name>::TABLE)
                ])
            }

            fn is_value(value: &::surrealdb::types::Value) -> bool {
                ::core::matches!(
                    value,
                    ::surrealdb::types::Value::RecordId(r)
                        if r.table.as_str() == <$name>::TABLE
                )
            }

            fn into_value(self) -> ::surrealdb::types::Value {
                ::surrealdb::types::Value::RecordId(self.0)
            }

            fn from_value(
                value: ::surrealdb::types::Value,
            ) -> ::core::result::Result<Self, ::surrealdb::Error>
            where
                Self: ::core::marker::Sized,
            {
                match value {
                    ::surrealdb::types::Value::RecordId(r)
                        if r.table.as_str() == <$name>::TABLE =>
                    {
                        ::core::result::Result::Ok(Self(r))
                    }
                    other => ::core::result::Result::Err(::surrealdb::Error::internal(
                        ::std::format!(
                            "expected {} record, got {other:?}",
                            <$name>::TABLE
                        ),
                    )),
                }
            }
        }
    };
}
