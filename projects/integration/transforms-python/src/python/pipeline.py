from transforms.api import Pipeline

from python import pnl, exposure, devops, transform_test_harness


my_pipeline = Pipeline()
my_pipeline.discover_transforms(pnl, exposure, devops, transform_test_harness)
