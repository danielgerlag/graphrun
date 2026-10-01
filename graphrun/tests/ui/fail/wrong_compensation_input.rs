use graphrun::builder::{ActivityRef, RegionBuilder};

fn wrong_compensation_input(
    root: &mut RegionBuilder<String>,
    forward: &ActivityRef<String, i64>,
    undo: &ActivityRef<String, String>,
) {
    let node = root.activity("forward", forward, root.input()).unwrap();
    root.compensate(&node, undo).unwrap();
}

fn main() {
    let _ = wrong_compensation_input;
}
