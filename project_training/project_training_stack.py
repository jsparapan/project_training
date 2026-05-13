from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    BundlingOptions,
    aws_ec2 as ec2,
    aws_kinesis as kinesis,
    aws_rds as rds,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    aws_s3 as s3,
    aws_glue as glue,
    CfnOutput,
)
from constructs import Construct


class ProjectTrainingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ------------------------------------------------------------------ #
        # VPC — two private subnets (Aurora) + two public (NAT for Lambda)    #
        # ------------------------------------------------------------------ #
        vpc = ec2.Vpc(
            self, "OrderVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                ),
            ],
        )

        # ------------------------------------------------------------------ #
        # Security groups                                                      #
        # ------------------------------------------------------------------ #
        lambda_sg = ec2.SecurityGroup(
            self, "LambdaSG",
            vpc=vpc,
            description="Security group for Lambda functions",
            allow_all_outbound=True,
        )

        aurora_sg = ec2.SecurityGroup(
            self, "AuroraSG",
            vpc=vpc,
            description="Security group for Aurora cluster",
            allow_all_outbound=False,
        )
        # Allow Lambda to connect to Aurora on Postgres port
        aurora_sg.add_ingress_rule(
            peer=lambda_sg,
            connection=ec2.Port.tcp(5432),
            description="Lambda to Aurora",
        )

        glue_sg = ec2.SecurityGroup(
            self, "GlueSG",
            vpc=vpc,
            description="Security group for Glue jobs",
            allow_all_outbound=True,
        )

        # Glue workers must be able to reach each other (Spark RPC)
        glue_sg.add_ingress_rule(
            peer=glue_sg,
            connection=ec2.Port.all_traffic(),
            description="Glue workers self-referencing - required for Spark",
        )

        # Allow Glue to connect to Aurora
        aurora_sg.add_ingress_rule(
            peer=glue_sg,
            connection=ec2.Port.tcp(5432),
            description="Glue to Aurora",
        )

        # ------------------------------------------------------------------ #
        # Kinesis Data Stream                                                  #
        # ------------------------------------------------------------------ #
        order_stream = kinesis.Stream(
            self, "OrderStream",
            stream_name="order-events",
            shard_count=1,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.MANAGED,
        )

        # ------------------------------------------------------------------ #
        # Aurora Serverless v2 — Postgres                                     #
        # ------------------------------------------------------------------ #
        aurora_cluster = rds.DatabaseCluster(
            self, "OrdersDb",
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_16_13
            ),
            serverless_v2_min_capacity=0.5,
            serverless_v2_max_capacity=4,
            writer=rds.ClusterInstance.serverless_v2("writer"),
            readers=[
                rds.ClusterInstance.serverless_v2("reader", scale_with_writer=True),
            ],
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            security_groups=[aurora_sg],
            default_database_name="orders",
            credentials=rds.Credentials.from_generated_secret("orders_admin"),
            removal_policy=RemovalPolicy.DESTROY,  # change to RETAIN for prod
        )

        # ------------------------------------------------------------------ #
        # RDS Proxy — Lambda talks to Aurora through this to pool connections  #
        # ------------------------------------------------------------------ #
        rds_proxy = rds.DatabaseProxy(
            self, "OrdersProxy",
            proxy_target=rds.ProxyTarget.from_cluster(aurora_cluster),
            secrets=[aurora_cluster.secret],
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[aurora_sg],
            require_tls=True,
            debug_logging=False,
        )

        # ------------------------------------------------------------------ #
        # SNS — order confirmation topic                                       #
        # ------------------------------------------------------------------ #
        confirmation_topic = sns.Topic(
            self, "OrderConfirmationTopic",
            topic_name="order-confirmations",
            display_name="Order Confirmation Notifications",
        )

        # ------------------------------------------------------------------ #
        # SQS — dead-letter queue for Lambda failures                         #
        # ------------------------------------------------------------------ #
        dlq = sqs.Queue(
            self, "OrderDLQ",
            queue_name="order-events-dlq",
            retention_period=Duration.days(14),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        # Main processing queue (for async work in later phases)
        order_queue = sqs.Queue(
            self, "OrderQueue",
            queue_name="order-processing",
            visibility_timeout=Duration.seconds(300),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlq,
            ),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        # ------------------------------------------------------------------ #
        # S3 bucket — Glue / Iceberg output                                   #
        # ------------------------------------------------------------------ #
        data_lake_bucket = s3.Bucket(
            self, "DataLakeBucket",
            bucket_name=f"order-data-lake-{self.account}-{self.region}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="archive-after-90-days",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(90),
                        )
                    ],
                )
            ],
        )

        # ------------------------------------------------------------------ #
        # IAM role — Lambda execution                                          #
        # ------------------------------------------------------------------ #
        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )
        order_stream.grant_read(lambda_role)
        aurora_cluster.secret.grant_read(lambda_role)
        confirmation_topic.grant_publish(lambda_role)
        order_queue.grant_send_messages(lambda_role)
        rds_proxy.grant_connect(lambda_role, db_user="orders_admin")

        order_processor_fn = lambda_.Function(
            self, "OrderProcessorFn",
            function_name="order-processor",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            # Point at the real code folder instead of Code.from_inline(...)
            code=lambda_.Code.from_asset(
                "lambda_query",
                bundling=BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && "
                        "cp -au . /asset-output",
                    ],
                ),
            ),
            role=lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[lambda_sg],
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={
                "DB_PROXY_ENDPOINT": rds_proxy.endpoint,
                "DB_SECRET_ARN":     aurora_cluster.secret.secret_arn,
                "SNS_TOPIC_ARN":     confirmation_topic.topic_arn,
                "DLQ_URL":           dlq.queue_url,
                "DB_NAME":           "orders",
            },
            dead_letter_queue=dlq,
        )

        # Wire Kinesis → Lambda
        order_processor_fn.add_event_source(
            lambda_events.KinesisEventSource(
                order_stream,
                starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                batch_size=10,
                bisect_batch_on_error=True,
                retry_attempts=2,
            )
        )

        # IAM role for query Lambda (read-only — no SNS/SQS needed)
        query_lambda_role = iam.Role(
            self, "QueryLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaVPCAccessExecutionRole"
                ),
            ],
        )
        aurora_cluster.secret.grant_read(query_lambda_role)
        rds_proxy.grant_connect(query_lambda_role, db_user="orders_admin")
        
        # Query Lambda — reads orders from Aurora, returns JSON
        query_fn = lambda_.Function(
            self, "OrderQueryFn",
            function_name="order-query",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="query_handler.lambda_handler",
            code=lambda_.Code.from_asset(
                "lambda_query",
                bundling=BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && "
                        "cp -r . /asset-output",
                    ],
                ),
            ),
            role=query_lambda_role,
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[lambda_sg],
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "DB_PROXY_ENDPOINT": rds_proxy.endpoint,
                "DB_SECRET_ARN":     aurora_cluster.secret.secret_arn,
                "DB_NAME":           "orders",
            },
        )
        
        # Lambda Function URL — public HTTPS endpoint Kong will proxy to
        query_fn_url = query_fn.add_function_url(
            auth_type=lambda_.FunctionUrlAuthType.NONE,  # Kong handles auth
            cors=lambda_.FunctionUrlCorsOptions(
                allowed_origins=["*"],
                allowed_methods=[lambda_.HttpMethod.GET],
            ),
        )
        
        # Output the URL — you'll paste this into kong_setup.sh
        CfnOutput(self, "QueryFunctionUrl",
                value=query_fn_url.url,
                description="Lambda Function URL — paste into kong_setup.sh")

        # ------------------------------------------------------------------ #
        # Glue VPC Connection (Network)                                      #
        # ------------------------------------------------------------------ #
        # Select a private subnet with NAT routing so Glue can reach both
        # Aurora (internal) AND Secrets Manager / S3 (external)
        glue_subnet = vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        ).subnets[0]

        glue_vpc_connection = glue.CfnConnection(
            self, "GlueVpcConnection",
            catalog_id=self.account,
            connection_input=glue.CfnConnection.ConnectionInputProperty(
                name="orders-vpc-connection",
                connection_type="NETWORK",
                physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                    security_group_id_list=[glue_sg.security_group_id],
                    subnet_id=glue_subnet.subnet_id,
                    availability_zone=glue_subnet.availability_zone,
                ),
            ),
        )

        # ------------------------------------------------------------------ #
        # Glue IAM role                                                        #
        # ------------------------------------------------------------------ #
        glue_role = iam.Role(
            self, "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )
        data_lake_bucket.grant_read_write(glue_role)
        aurora_cluster.secret.grant_read(glue_role)

        # ------------------------------------------------------------------ #
        # Glue database (Iceberg catalog namespace)                            #
        # ------------------------------------------------------------------ #
        glue_db = glue.CfnDatabase(
            self, "OrdersGlueDb",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="orders_catalog",
                description="Order pipeline Iceberg tables",
            ),
        )

        # Glue job definition (script uploaded in Phase 3)
        glue_job = glue.CfnJob(
            self, "OrdersExportJob",
            name="orders-to-iceberg",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{data_lake_bucket.bucket_name}/scripts/orders_to_iceberg.py",
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[glue_vpc_connection.ref]
            ),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            default_arguments={
                "--enable-glue-datacatalog": "true",
                "--datalake-formats": "iceberg",
                "--additional-python-modules": "boto3",
                "--conf": (
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions"
                    ".IcebergSparkSessionExtensions"
                    " --conf spark.sql.catalog.glue_catalog=org.apache.iceberg"
                    ".spark.SparkCatalog"
                    " --conf spark.sql.catalog.glue_catalog.warehouse="
                    f"s3://{data_lake_bucket.bucket_name}/iceberg/"
                    " --conf spark.sql.catalog.glue_catalog.catalog-impl="
                    "org.apache.iceberg.aws.glue.GlueCatalog"
                ),
                "--DATABASE_NAME": "orders_catalog",
                "--TABLE_NAME": "orders",
                "--S3_OUTPUT": f"s3://{data_lake_bucket.bucket_name}/iceberg/",
                "--DB_SECRET_ARN": aurora_cluster.secret.secret_arn,
            },
        )

        # ------------------------------------------------------------------ #
        # EventBridge rule — trigger Glue job on a schedule                   #
        # ------------------------------------------------------------------ #
        glue_target = targets.LambdaFunction  # we use a small trigger Lambda

        glue_trigger_fn = lambda_.Function(
            self, "GlueTriggerFn",
            function_name="glue-job-trigger",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_inline(
                "import boto3, os\n"
                "def handler(event, context):\n"
                "    client = boto3.client('glue')\n"
                "    resp = client.start_job_run(JobName=os.environ['GLUE_JOB_NAME'])\n"
                "    print('Started Glue run:', resp['JobRunId'])\n"
                "    return resp['JobRunId']\n"
            ),
            timeout=Duration.seconds(30),
            environment={
                "GLUE_JOB_NAME": "orders-to-iceberg",
            },
        )
        glue_trigger_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun"],
                resources=[f"arn:aws:glue:{self.region}:{self.account}:job/orders-to-iceberg"],
            )
        )

        daily_export_rule = events.Rule(
            self, "DailyExportRule",
            rule_name="daily-orders-export",
            description="Trigger Glue export of orders to Iceberg daily at 02:00 UTC",
            schedule=events.Schedule.cron(hour="2", minute="0"),
        )
        daily_export_rule.add_target(
            targets.LambdaFunction(glue_trigger_fn)
        )

        # 1. OIDC Identity Provider for GitHub Actions
        github_oidc_provider = iam.OpenIdConnectProvider(
            self, "GitHubOidcProvider",
            url="https://token.actions.githubusercontent.com",
            client_ids=["sts.amazonaws.com"],
            thumbprints=["6938fd4d98bab03faadb97b34396831e3780aea1"],
        )
        
        # 2. IAM Role that GitHub Actions will assume via OIDC
        github_actions_role = iam.Role(
            self, "GitHubActionsCDKRole",
            role_name="GitHubActionsCDKRole",
            assumed_by=iam.WebIdentityPrincipal(
                github_oidc_provider.open_id_connect_provider_arn,
                conditions={
                    "StringLike": {
                        "token.actions.githubusercontent.com:sub":
                            "repo:jsparapan/project_training:*",
                    },
                    "StringEquals": {
                        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
                    },
                },
            ),
            description="Role assumed by GitHub Actions for CDK deployments",
            max_session_duration=Duration.hours(1),
        )
        
        # 3. Grant the role permissions to deploy the stack
        # AdministratorAccess is used here for simplicity in training.
        # In production, scope this down to only the services your stack uses.
        github_actions_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")
        )
        
        # Output the role ARN — you'll see this after cdk deploy
        CfnOutput(self, "GitHubActionsRoleArn",
                value=github_actions_role.role_arn,
                description="IAM Role ARN for GitHub Actions OIDC")

        # ------------------------------------------------------------------ #
        # CloudFormation outputs — handy when running Phase 2 / 3             #
        # ------------------------------------------------------------------ #
        CfnOutput(self, "KinesisStreamName",
                  value=order_stream.stream_name,
                  description="Put records here to test the pipeline")

        CfnOutput(self, "AuroraClusterEndpoint",
                  value=aurora_cluster.cluster_endpoint.hostname,
                  description="Direct Aurora writer endpoint (for admin)")

        CfnOutput(self, "RdsProxyEndpoint",
                  value=rds_proxy.endpoint,
                  description="Proxy endpoint Lambda should use")

        CfnOutput(self, "AuroraSecretArn",
                  value=aurora_cluster.secret.secret_arn,
                  description="Secrets Manager ARN for DB credentials")

        CfnOutput(self, "ConfirmationTopicArn",
                  value=confirmation_topic.topic_arn,
                  description="SNS topic for order confirmations")

        CfnOutput(self, "OrderQueueUrl",
                  value=order_queue.queue_url,
                  description="SQS queue for async order processing")

        CfnOutput(self, "DlqUrl",
                  value=dlq.queue_url,
                  description="Dead-letter queue for failed events")

        CfnOutput(self, "DataLakeBucketOutput",
          value=data_lake_bucket.bucket_name,
          description="S3 bucket for Glue / Iceberg output")