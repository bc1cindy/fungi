// `cost-function` has no code yet, but CI runs `cargo nextest` and a coverage
// gate over this workspace: nextest exits non-zero on "no tests to run", and
// the gate demands 100% line coverage, which it cannot compute from zero
// lines. This placeholder satisfies both. Delete it once there are real tests.
#[cfg(test)]
mod tests {
    #[test]
    fn placeholder() {}
}
