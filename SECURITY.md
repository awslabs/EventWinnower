# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in EventWinnower, please email awssecurityopensourcelabs@amazon.com with:
- Description of the vulnerability
- Steps to reproduce (if applicable)
- Potential impact

Please do not open a public GitHub issue for security vulnerabilities.

We aim to acknowledge reports within 48 hours and provide updates on remediation progress.

## Supported Versions

| Version | Supported          |
|---------|-------------------|
| 0.1.x   | :white_check_mark: |

## Security Considerations

### AWS Credentials
- EventWinnower uses AWS SDK for S3, SQS, Kinesis, Firehose, SNS, Athena, and DynamoDB operations
- Ensure AWS credentials are properly configured and have minimal required permissions
- Never commit credentials to version control
- Use IAM roles when running in AWS environments (Lambda, EC2, etc.)

### Input Validation
- JMESPath expressions are validated by the jmespath library
- Regex patterns are compiled and validated at runtime
- User-provided field names and paths are validated before processing

### Data Processing
- Data is processed in-memory during pipeline execution
- Consider memory constraints when processing large datasets
- Use thread pool mode for I/O-heavy workloads to manage resource usage

### Dependency Management
- Dependencies are regularly audited using `cargo audit`
- Security updates are prioritized and released promptly

## Best Practices

1. **Principle of Least Privilege** - Grant AWS IAM roles only the permissions needed
2. **Validate Untrusted Input** - Be cautious with user-provided JMESPath expressions and regex patterns
3. **Monitor Resource Usage** - Set appropriate thread counts and monitor memory usage
4. **Keep Dependencies Updated** - Regularly update EventWinnower and its dependencies
