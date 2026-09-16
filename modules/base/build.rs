fn main() {
    // `sqlx::migrate!` embeds `database/migrations` into `db::MIGRATOR` at compile
    // time, but Cargo cannot see that the macro read the directory: without this, a
    // new migration leaves every binary built from a cached `base` without it.
    println!("cargo:rerun-if-changed=../../database/migrations");
}
