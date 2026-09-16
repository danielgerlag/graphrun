#[test]
fn ui() {
    let version = String::from_utf8(
        std::process::Command::new("rustc")
            .arg("-V")
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();
    if !version.contains("1.96.") {
        eprintln!(
            "trybuild stderr is recorded against rustc 1.96; skipping snapshots on {version}"
        );
        return;
    }
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/fail/*.rs");
}
