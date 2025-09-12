from aws_cdk import aws_iam
from constructs import Construct
from aws_cdk import aws_s3 as s3
from aws_cdk import Stack, aws_lambda, Duration
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets


class ApiConsumerStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        results_bucket = s3.Bucket(self, "ApiConsumerResultsBucket")

        randomUserFn = aws_lambda.Function(
            self,
            "HttpConsumerRandomUserFunction",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="handler_with_proxy.consume_api",
            timeout=Duration.seconds(60),
            code=aws_lambda.Code.from_asset("lambda"),
            environment={
                "ENDPOINT_URL": "https://randomuser.me/api/?results=100",
                "S3_BUCKET": results_bucket.bucket_name,
                "S3_PREFIX": "randomuser/",
            },
        )
        
        randomUserFn.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[
                    "arn:aws:secretsmanager:*:*:secret:PROXY_URL*",
                ],
            )
        )

        jsonPlaceholderFn = aws_lambda.Function(
            self,
            "HttpConsumerJSONPlaceholderFunction",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="handler.consume_api",
            timeout=Duration.seconds(60),
            code=aws_lambda.Code.from_asset("lambda"),
            environment={
                "ENDPOINT_URL": "https://jsonplaceholder.typicode.com/users",
                "S3_BUCKET": results_bucket.bucket_name,
                "S3_PREFIX": "jsonplaceholder/",
            },
        )

        results_bucket.grant_put(randomUserFn)
        results_bucket.grant_put(jsonPlaceholderFn)

        dw_arn = (
            f"arn:aws:lambda:{Stack.of(self).region}:336392948345:layer:AWSSDKPandas-Python311:10"
        )
        dw_layer = aws_lambda.LayerVersion.from_layer_version_arn(self, "DataWranglerLayer", dw_arn)
        randomUserFn.add_layers(dw_layer)
        jsonPlaceholderFn.add_layers(dw_layer)

        # Scheduled triggers (EventBridge)
        events.Rule(
            self,
            "RandomUserSchedule",
            schedule=events.Schedule.rate(Duration.minutes(60 * 24)),
            targets=[targets.LambdaFunction(randomUserFn)],
        )

        events.Rule(
            self,
            "JsonPlaceholderSchedule",
            schedule=events.Schedule.rate(Duration.minutes(60 * 24)),
            targets=[targets.LambdaFunction(jsonPlaceholderFn)],
        )
