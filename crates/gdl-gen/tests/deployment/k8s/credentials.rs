use super::*;

#[test]
fn static_aws_credentials_do_not_reach_output_files() {
    let mirror = Mirror::new();
    let spec = placed(&mirror, SPLIT);
    let marker = "review-aws-secret-marker";
    let text = unsecret().replace(
        "RUST_LOG = \"info\"",
        &format!("AWS_ACCESS_KEY_ID = \"review-access-key\", AWS_SECRET_ACCESS_KEY = \"{marker}\""),
    );
    match render(&mirror, &spec, &fleet(&text)) {
        Err(error) => assert!(error.to_string().contains("AWS_"), "{error}"),
        Ok(files) => {
            for (path, text) in files {
                assert!(
                    !text.contains(marker),
                    "{} contains the static AWS secret marker",
                    path.display()
                );
            }
            panic!("a fleet with static AWS credentials was accepted");
        }
    }
}
