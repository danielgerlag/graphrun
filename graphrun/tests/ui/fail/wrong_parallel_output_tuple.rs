use graphrun::builder::{Branch, NodeRef, RegionBuilder};
use graphrun::region;

fn wrong_parallel_output_tuple(root: &mut RegionBuilder<String>) {
    let input = root.input().binding().clone();
    let output = root
        .parallel(
            "parallel",
            (
                Branch {
                    name: "text".into(),
                    input: input.clone(),
                    body: region::<String>().finish().unwrap(),
                },
                Branch {
                    name: "number".into(),
                    input,
                    body: region::<i64>().finish().unwrap(),
                },
            ),
        )
        .unwrap();
    let _: NodeRef<(i64, String)> = output;
}

fn main() {
    let _ = wrong_parallel_output_tuple;
}
