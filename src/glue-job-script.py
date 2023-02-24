import sys

from awsglue.transforms import Map
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

print("______________start of script")

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'inputPath', 'inputKey', 'outputPath', 'outputKey'])
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
glueContext = GlueContext(spark)
print("______________context is running")


source_path = f"s3://{args['inputPath']}/{args['inputKey']}"
target_path = f"s3://{args['outputPath']}/{args['outputKey']}"
print("INPPPPPPPUT1" + source_path)
print("PUTPPPPPPPPUT1" + source_path)

source_frame = glueContext.read.format('csv').load(source_path)

#
# def to_upper(row):
#     print("################# START PROCESSING")
#     print(row)
#     return {col: row[col].upper() for col in row}
#
#
# upper_case_transform = Map.apply(frame=source_frame, f=to_upper)
#
# target_frame = DynamicFrame.fromDF(upper_case_transform.toDF(), glueContext, "transformed")
#
# target_spec = {"writeToFileFormat": "csv", "separator": ",", "quoteChar": '"', "fileExtension": "csv"}
# glueContext.write_dynamic_frame.from_options(frame=source_frame.toDF(), connection_type="s3", connection_options={"path": target_path}, format=target_spec)
