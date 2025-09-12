from aws_cdk import aws_iam
from constructs import Construct
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_glue as glue
from aws_cdk import aws_athena as athena
from aws_cdk import aws_lakeformation as lf
from aws_cdk.aws_events import Rule, Schedule
from aws_cdk import Stack, Duration, aws_lambda
from aws_cdk.aws_events_targets import LambdaFunction

class RandomUserConsumerStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # create the bucket to store response data
        results_bucket = s3.Bucket(self, "JsonRandomUserResultsBucket")
        
        # deploy the function to consume API endpoint 
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

        # add wrangler layer to the lambda
        arn_layer = self.node.try_get_context("wrangler_layer")
        dw_layer = aws_lambda.LayerVersion.from_layer_version_arn(self, "DataWranglerLayer", arn_layer)
        randomUserFn.add_layers(dw_layer)

        # add permission to lambda put values in bucket
        results_bucket.grant_put(randomUserFn)

        # create a rule for schenduled trigger function
        Rule(
            self,
            "RamdonUserSchedule",
            schedule=Schedule.rate(Duration.minutes(60 * 24)),
            targets=[LambdaFunction(randomUserFn)],
        )

        # create a glue database 
        glue_db_name = "random_user_db"
        glue_db = glue.CfnDatabase(
            self,
            "RandomUserGlueDatabase",
            catalog_id=Stack.of(self).account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(name=glue_db_name),
        )

        glue.CfnTable(
            self,
            "RandomUserGlueTable",
            catalog_id=Stack.of(self).account,
            database_name=glue_db_name,  # "random_user_db"
            table_input=glue.CfnTable.TableInputProperty(
                name="api_consumer_randomuser",
                table_type="EXTERNAL_TABLE",
                parameters={"classification": "json"},
                partition_keys=[
                    glue.CfnTable.ColumnProperty(name="partition_0", type="string"),
                    glue.CfnTable.ColumnProperty(name="partition_1", type="string"),
                    glue.CfnTable.ColumnProperty(name="partition_2", type="string"),
                ],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{results_bucket.bucket_name}/randomuser/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.openx.data.jsonserde.JsonSerDe"
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="gender", type="string"),
                        glue.CfnTable.ColumnProperty(name="email", type="string"),
                        glue.CfnTable.ColumnProperty(name="phone", type="string"),
                        glue.CfnTable.ColumnProperty(name="cell", type="string"),
                        glue.CfnTable.ColumnProperty(name="nat", type="string"),
                        glue.CfnTable.ColumnProperty(name="name_title", type="string"),
                        glue.CfnTable.ColumnProperty(name="name_first", type="string"),
                        glue.CfnTable.ColumnProperty(name="name_last", type="string"),
                        glue.CfnTable.ColumnProperty(name="location_street_number", type="smallint"),
                        glue.CfnTable.ColumnProperty(name="location_street_name", type="string"),
                        glue.CfnTable.ColumnProperty(name="location_city", type="string"),
                        glue.CfnTable.ColumnProperty(name="location_state", type="string"),
                        glue.CfnTable.ColumnProperty(name="location_country", type="string"),
                        glue.CfnTable.ColumnProperty(name="location_postcode", type="string"),
                        glue.CfnTable.ColumnProperty(name="location_coordinates_latitude", type="double"),
                        glue.CfnTable.ColumnProperty(name="location_coordinates_longitude", type="double"),
                        glue.CfnTable.ColumnProperty(name="location_timezone_offset", type="string"),
                        glue.CfnTable.ColumnProperty(name="location_timezone_description", type="string"),
                        glue.CfnTable.ColumnProperty(name="login_uuid", type="string"),
                        glue.CfnTable.ColumnProperty(name="login_username", type="string"),
                        glue.CfnTable.ColumnProperty(name="login_password", type="string"),
                        glue.CfnTable.ColumnProperty(name="login_salt", type="string"),
                        glue.CfnTable.ColumnProperty(name="login_md5", type="string"),
                        glue.CfnTable.ColumnProperty(name="login_sha1", type="string"),
                        glue.CfnTable.ColumnProperty(name="login_sha256", type="string"),
                        glue.CfnTable.ColumnProperty(name="dob_date", type="timestamp"),
                        glue.CfnTable.ColumnProperty(name="dob_age", type="smallint"),
                        glue.CfnTable.ColumnProperty(name="registered_date", type="timestamp"),
                        glue.CfnTable.ColumnProperty(name="registered_age", type="smallint"),
                        glue.CfnTable.ColumnProperty(name="id_name", type="string"),
                        glue.CfnTable.ColumnProperty(name="id_value", type="string"),
                        glue.CfnTable.ColumnProperty(name="picture_large", type="string"),
                        glue.CfnTable.ColumnProperty(name="picture_medium", type="string"),
                        glue.CfnTable.ColumnProperty(name="picture_thumbnail", type="string")
                    ]
                ),
            ),
        )

        # Allow glue to read S3 data
        glue_role = aws_iam.Role(
            self,
            "ApiRandomUserGlueCrawlerRole",
            assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
            ],
        )
        results_bucket.grant_read(glue_role)

        # create a crawler to ingest s3 files to glue
        crawler = glue.CfnCrawler(
            self,
            "ApiRandomUserGlueCrawler",
            role=glue_role.role_arn,
            database_name=glue_db_name,
            table_prefix="api_consumer_",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(path=f"s3://{results_bucket.bucket_name}/randomuser/"),
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
            "RandomUserAthenaQueryRole",
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

        # allow athena role to DESCRIBE the specific table (required for UI listing)
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
            permissions=["DESCRIBE"],
        )

        # Grant column-level SELECT permissions to Athena on specific columns
        lf.CfnPermissions(
            self,
            "LfPermsAthenaSelectRandomUserColumns",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=athena_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                table_with_columns_resource=lf.CfnPermissions.TableWithColumnsResourceProperty(
                    catalog_id=Stack.of(self).account,
                    database_name=glue_db_name,
                    name="api_consumer_randomuser",
                    column_wildcard=lf.CfnPermissions.ColumnWildcardProperty(
                        excluded_column_names=[
                            # EXCLUIR sensibles ->
                            "login_password", "login_md5", "login_sha1", "login_sha256",
                            "id_value",  # identidades
                            "picture_large", "picture_medium", "picture_thumbnail"  # PII/URLs
                        ]
                    )
                )
            ),
            permissions=["SELECT"],
        )

        
        athena_results_bucket = s3.Bucket(self, "RandomUserAthenaResultsBucket")

        # Allow the Athena role to read/write query results
        athena_results_bucket.grant_read_write(athena_role)

        # Create an Athena WorkGroup with S3 results location
        athena.CfnWorkGroup(
            self,
            "RandomUserAthenaWorkGroup",
            name="RandomUserWG",
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