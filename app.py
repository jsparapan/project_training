#!/usr/bin/env python3
import aws_cdk as cdk
from project_training.project_training_stack import ProjectTrainingStack

app = cdk.App()

ProjectTrainingStack(
    app,
    "ProjectTrainingStack",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "eu-west-1",
    ),
)

app.synth()