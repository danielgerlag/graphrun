#[test]
fn ui() {
    for case in [
        "wrong_activity_input",
        "wrong_loop_carry",
        "wrong_region_return",
        "wrong_compensation_input",
        "wrong_parallel_output_tuple",
    ] {
        assert!(
            std::path::Path::new("tests/ui/fail")
                .join(format!("{case}.rs"))
                .is_file(),
            "missing compile-fail case {case}"
        );
    }
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/fail/*.rs");
}
