use graphrun::builder::{Region, RegionBuilder};

fn wrong_region_return() -> Region<String, i64> {
    let body = RegionBuilder::<String>::new();
    body.complete("done", body.input()).unwrap()
}

fn main() {
    let _ = wrong_region_return;
}
