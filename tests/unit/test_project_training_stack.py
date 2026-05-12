import aws_cdk as core
import aws_cdk.assertions as assertions

from project_training.project_training_stack import ProjectTrainingStack

# example tests. To run these tests, uncomment this file along with the example
# resource in project_training/project_training_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ProjectTrainingStack(app, "project-training")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
