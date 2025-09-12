from aws_cdk import aws_iam
from constructs import Construct
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_glue as glue
from aws_cdk import aws_athena as athena
from aws_cdk import aws_lakeformation as lf
from aws_cdk.aws_events import Rule, Schedule
from aws_cdk import Stack, Duration, aws_lambda
from aws_cdk.aws_events_targets import LambdaFunction

class JsonPlaceHolderConsumerStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # create the bucket to store response data
        results_bucket = s3.Bucket(self, "JsonPlaceholderConsumerResultsBucket")
        
        # deploy the function to consume API endpoint 
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

        # add wrangler layer to the lambda
        arn_layer = self.node.try_get_context("wrangler_layer")
        dw_layer = aws_lambda.LayerVersion.from_layer_version_arn(self, "DataWranglerLayer", arn_layer)
        jsonPlaceholderFn.add_layers(dw_layer)

        # add permission to lambda put values in bucket
        results_bucket.grant_put(jsonPlaceholderFn)

        # create a rule for schenduled trigger function
        Rule(
            self,
            "JsonPlaceholderSchedule",
            schedule=Schedule.rate(Duration.minutes(60 * 24)),
            targets=[LambdaFunction(jsonPlaceholderFn)],
        )

        # create a glue database 
        glue_db_name = "json_placeholder_db"
        glue_db = glue.CfnDatabase(
            self,
            "JsonPlaceholderGlueDatabase",
            catalog_id=Stack.of(self).account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(name=glue_db_name),
        )

        # Allow glue to read S3 data
        glue_role = aws_iam.Role(
            self,
            "ApiJsonPlaceholderGlueCrawlerRole",
            assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
            ],
        )
        results_bucket.grant_read(glue_role)

        # create a crawler to ingest s3 files to glue
        crawler = glue.CfnCrawler(
            self,
            "ApiJsonPlaceholderGlueCrawler",
            role=glue_role.role_arn,
            database_name=glue_db_name,
            table_prefix="api_consumer_",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(path=f"s3://{results_bucket.bucket_name}/jsonplaceholder/"),
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression="cron(0 1 * * ? *)"),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="LOG",
                update_behavior="UPDATE_IN_DATABASE",
            ),
        )
        crawler.add_dependency(glue_db)


        # Register S3 location in lake formation
        lf.CfnResource(
            self,
            "LfRegisterBucket",
            resource_arn=f"arn:aws:s3:::{results_bucket.bucket_name}",
            use_service_linked_role=True,
        )

        # Grant Data Location permissions to Glue
        lf.CfnPermissions(
            self,
            "LfPermsDataLocationCrawler",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=glue_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                data_location_resource=lf.CfnPermissions.DataLocationResourceProperty(
                    s3_resource=f"arn:aws:s3:::{results_bucket.bucket_name}"
                )
            ),
            permissions=["DATA_LOCATION_ACCESS"],
        )

        # Database-level perms for crawler to create/update tables
        lf.CfnPermissions(
            self,
            "LfPermsDatabaseCrawler",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=glue_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=Stack.of(self).account,
                    name=glue_db_name,
                )
            ),
            permissions=["CREATE_TABLE", "ALTER", "DROP", "DESCRIBE"],
        )

        # Create an Athena query role and grant Lake Formation permissions to query the data
        athena_role = aws_iam.Role(
            self,
            "JsonPlaceholderAthenaQueryRole",
            assumed_by=aws_iam.AccountPrincipal(account_id=Stack.of(self).account),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess"),
            ],
        )
        # Give S3 read permissions for query outputs and data (read-only)
        results_bucket.grant_read(athena_role)
    
        # allow Athena role to DESCRIBE DB and SELECT on all tables in DB
        lf.CfnPermissions(
            self,
            "LfPermsDatabaseAthenaDescribe",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=athena_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=Stack.of(self).account,
                    name=glue_db_name,
                )
            ),
            permissions=["DESCRIBE"],
        )

        # allow athena to call all columns in table 
        lf.CfnPermissions(
            self,
            "LfPermsTablesAthenaSelectAll",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=athena_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                table_resource=lf.CfnPermissions.TableResourceProperty(
                    catalog_id=Stack.of(self).account,
                    database_name=glue_db_name,
                    table_wildcard={},
                )
            ),
            permissions=["SELECT", "DESCRIBE"],
        )
        athena_results_bucket = s3.Bucket(self, "JsonPlaceholderAthenaResultsBucket")

        # Allow the Athena role to read/write query results
        athena_results_bucket.grant_read_write(athena_role)

        # Create an Athena WorkGroup with S3 results location
        athena.CfnWorkGroup(
            self,
            "JsonPlaceholderAthenaWorkGroup",
            name="JsonPlaceholderWG",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{athena_results_bucket.bucket_name}/results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                ),
            ),
            state="ENABLED",
        )

       