# AWS Proton Sample Lambda Data Processing Service

This sample application contains AWS Lambda function code to handle data processing engine that consumes data from kinesis streams and pushes it into a firehose. This sample can be deployed with AWS Proton using the sample [Lambda Multi Service](https://github.com/aws-samples/aws-proton-sample-templates/tree/main/lambda-multi-svc) environment and service templates (this one specifically goes along with the service-data-processing template).

With the sample Proton templates, you can provide a simple `noun` for the API like `tasks`, and Proton will create a callback handler that receives updates for tasks and pushes them to a kinesis stream, and then a handler that consumes data from that stream and pushes it to a kinesis firehose. 

## Testing

Run unit tests with the `python -m unittest` command or `make test`.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
