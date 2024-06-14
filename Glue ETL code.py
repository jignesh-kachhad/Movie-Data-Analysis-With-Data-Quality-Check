import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
    def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_dyf, group):
    return Filter.apply(frame=source_dyf, f=group.filters)

def threaded_route(glue_ctx, source_dyf, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {
            executor.submit(apply_group_filter, source_dyf, gf): gf
            for gf in group_filters
        }
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print(f"{gf} generated an exception: {future.exception()}")
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# Script generated for node movies_from_s3
movies_dyf = glue_ctx.create_dynamic_frame.from_catalog(
    database="movies_metadata_db",
    table_name="movies_raw_input_data",
    transformation_ctx="movies_dyf",
)

# Script generated for node Evaluate Data Quality
dq_rules = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        IsComplete "imdb_rating",
        ColumnValues "imdb_rating" between 8.4 and 10.3
    ]
"""

dq_evaluation = EvaluateDataQuality().process_rows(
    frame=movies_dyf,
    ruleset=dq_rules,
    publishing_options={
        "dataQualityEvaluationContext": "DQEvaluation",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Script generated for node ruleOutcomes
rule_outcomes_dyf = SelectFromCollection.apply(
    dfc=dq_evaluation,
    key="ruleOutcomes",
    transformation_ctx="rule_outcomes_dyf",
)

# Script generated for node rowLevelOutcomes
row_outcomes_dyf = SelectFromCollection.apply(
    dfc=dq_evaluation,
    key="rowLevelOutcomes",
    transformation_ctx="row_outcomes_dyf",
)

# Script generated for node Conditional Router
group_filters = [
    GroupFilter(
        name="failed_records",
        filters=lambda row: bool(re.match("Failed", row["DataQualityEvaluationResult"])),
    ),
    GroupFilter(
        name="default_group",
        filters=lambda row: not bool(re.match("Failed", row["DataQualityEvaluationResult"])),
    ),
]

routed_dyf = threaded_route(
    glue_ctx,
    source_dyf=row_outcomes_dyf,
    group_filters=group_filters,
)

# Script generated for node failed_record
failed_records_dyf = SelectFromCollection.apply(
    dfc=routed_dyf,
    key="failed_records",
    transformation_ctx="failed_records_dyf",
)

# Script generated for node default_group
default_group_dyf = SelectFromCollection.apply(
    dfc=routed_dyf,
    key="default_group",
    transformation_ctx="default_group_dyf",
)

# Script generated for node Change Schema
mapped_dyf = ApplyMapping.apply(
    frame=default_group_dyf,
    mappings=[
        ("overview", "string", "overview", "string"),
        ("gross", "string", "gross", "string"),
        ("director", "string", "director", "string"),
        ("certificate", "string", "certificate", "string"),
        ("star4", "string", "star4", "string"),
        ("runtime", "string", "runtime", "string"),
        ("star2", "string", "star2", "string"),
        ("star3", "string", "star3", "string"),
        ("no_of_votes", "long", "no_of_votes", "int"),
        ("series_title", "string", "series_title", "string"),
        ("meta_score", "long", "meta_score", "int"),
        ("star1", "string", "star1", "string"),
        ("genre", "string", "genre", "string"),
        ("released_year", "string", "released_year", "string"),
        ("poster_link", "string", "poster_link", "string"),
        ("imdb_rating", "double", "imdb_rating", "decimal"),
    ],
    transformation_ctx="mapped_dyf",
)

# Script generated for node Amazon S3
glue_ctx.write_dynamic_frame.from_options(
    frame=rule_outcomes_dyf,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://movies-gds1/rule_outcome_from_etl/", "partitionKeys": []},
    transformation_ctx="rule_outcomes_s3",
)

# Script generated for node Amazon S3
glue_ctx.write_dynamic_frame.from_options(
    frame=failed_records_dyf,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://movies-gds1/bad_records/", "partitionKeys": []},
    transformation_ctx="failed_records_s3",
)

# Script generated for node AWS Glue Data Catalog
glue_ctx.write_dynamic_frame.from_catalog(
    frame=mapped_dyf,
    database="movies_metadata_db",
    table_name="dev_movies_imdb_movies_rating",
    redshift_tmp_dir="s3://redshift-temp-data-kcd/temp-data/movies/",
    additional_options={"aws_iam_role": "arn:aws:iam::126362963275:role/redshift-role"},
    transformation_ctx="catalog_output",
)

job.commit()
