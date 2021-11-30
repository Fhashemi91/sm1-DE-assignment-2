from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from iso3166 import countries
from pyspark.sql.types import StringType


builder = (
    SparkSession.builder.enableHiveSupport()
    .appName("assignment-2")
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )
)

spark = builder.getOrCreate()


@F.udf(returnType=StringType())
def country_code_to_alpha2(country_code_3):
    try:
        country = countries.get(country_code_3)
    except:
        return None
    return country.alpha2


@F.udf(returnType=StringType())
def country_code_to_name(country_code_2):
    try:
        country = countries.get(country_code_2)
    except:
        return None
    return country.name


def main(path="gs://covid19-cases/"):
    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("delimiter", ",")
        .load(path)
    )

    df = (
        # Fill empty cells with null
        df.select(
            [
                F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c)
                for c in df.columns
            ]
        )
        # Add unified country code and name columns
        .withColumn("country_code", country_code_to_alpha2(F.col("iso_code")))
        .withColumn("country", country_code_to_name(F.col("country_code")))
        # Make new column with partial vaccinated people minus fully vaccinated people
        .withColumn(
            "people_incomplete_vaccination",
            F.col("people_vaccinated") - F.col("people_fully_vaccinated"),
        )
        # Drop few columns with less than 1.5% non-null values
        .drop(
            "iso_code",
            "location",
            "weekly_icu_admissions",
            "weekly_icu_admissions_per_million",
            "weekly_hosp_admissions",
            "weekly_hosp_admissions_per_million",
        )
    )

    df.repartition("country_code").write.partitionBy("country_code").mode(
        "overwrite"
    ).format("bigquery").option("temporaryGcsBucket", "temp-gcs-bucket").save(
        "de_assignment_2.covid19_aws"
    )


if __name__ == "__main__":
    main()
