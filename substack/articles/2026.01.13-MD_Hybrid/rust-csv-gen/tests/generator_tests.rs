use std::fs;

#[test]
fn writes_small_inventory_and_orders() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().to_path_buf();

    // Tiny targets to keep tests fast
    rust_csv_gen::generator::generate_inventory_csv(&out_dir, "inventory", 10_000, 42).expect("inventory");
    rust_csv_gen::generator::generate_orders_csv_parallel(&out_dir, "orders", 50_000, 2, 42).expect("orders");

    // Inventory file exists
    let inv = out_dir.join("inventory.csv");
    assert!(inv.exists(), "inventory.csv should exist");
    let inv_size = fs::metadata(inv).expect("meta").len();
    assert!(inv_size >= 10_000, "inventory size should meet target");

    // Orders header and detail files exist
    let h0 = out_dir.join("orders_header_00.csv");
    let d0 = out_dir.join("orders_detail_00.csv");
    assert!(h0.exists(), "orders_header_00.csv should exist");
    assert!(d0.exists(), "orders_detail_00.csv should exist");
}
