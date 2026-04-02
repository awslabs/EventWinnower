use aws_config::{BehaviorVersion, ConfigLoader, SdkConfig};

#[allow(dead_code)]
/// get the AWS config based on environment
pub async fn get_aws_config() -> SdkConfig {
    aws_config::defaults(BehaviorVersion::latest()).load().await
}

#[allow(dead_code)]
/// if you see this function used, its probably best to just eliminate it and do a load directly
pub async fn get_aws_config_from_builder(builder: ConfigLoader) -> SdkConfig {
    builder.load().await
}
