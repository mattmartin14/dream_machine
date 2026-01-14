use std::fs;

#[test]
fn writes_small_inventory_and_orders() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().to_path_buf();

    // Tiny targets to keep tests fast
    let skus = rust_csv_gen::generator::generate_inventory_csv(&out_dir, "inventory", 10_000, 42).expect("inventory");
    rust_csv_gen::generator::generate_orders_csv_parallel(&out_dir, "orders", 50_000, 2, 42, &skus).expect("orders");

    // Inventory file exists
    let inv = out_dir.join("inventory.csv");
    assert!(inv.exists(), "inventory.csv should exist");
    let inv_size = fs::metadata(inv.clone()).expect("meta").len();
    assert!(inv_size >= 10_000, "inventory size should meet target");

    // Verify inventory has 7 columns (header and first data row)
    let inv_str = fs::read_to_string(inv).expect("read inv");
    let mut inv_lines = inv_str.lines();
    let header = inv_lines.next().expect("header");
    let first = inv_lines.next().expect("first row");
    assert_eq!(header.split(',').count(), 7, "inventory header must have 7 columns");
    assert_eq!(first.split(',').count(), 7, "inventory first row must have 7 columns");

    // Orders header and detail files exist
    let h0 = out_dir.join("orders_header_00.csv");
    let d0 = out_dir.join("orders_detail_00.csv");
    assert!(h0.exists(), "orders_header_00.csv should exist");
    assert!(d0.exists(), "orders_detail_00.csv should exist");
}
