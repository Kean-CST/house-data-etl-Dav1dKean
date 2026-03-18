"""
House Sale Data ETL Pipeline
============================
Implement the three functions below to complete the ETL pipeline.

Steps:
  1. EXTRACT  – load the CSV into a PySpark DataFrame
  2. TRANSFORM – split the data by neighborhood and save each as a separate CSV
  3. LOAD      – insert each neighborhood DataFrame into its own PostgreSQL table
"""
from __future__ import annotations
import shutil
import csv  # noqa: F401
import os  # noqa: F401
from pathlib import Path

from dotenv import load_dotenv  # noqa: F401
from pyspark.sql import DataFrame, SparkSession  # noqa: F401
from pyspark.sql import functions as F  # noqa: F401

# ── Predefined constants (do not modify) ──────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent

NEIGHBORHOODS = [
    "Downtown", "Green Valley", "Hillcrest", "Lakeside", "Maple Heights",
    "Oakwood", "Old Town", "Riverside", "Suburban Park", "University District",
]

OUTPUT_DIR   = ROOT / "output" / "by_neighborhood"
OUTPUT_FILES = {hood: OUTPUT_DIR / f"{hood.replace(' ', '_').lower()}.csv" for hood in NEIGHBORHOODS}

PG_TABLES = {hood: f"public.{hood.replace(' ', '_').lower()}" for hood in NEIGHBORHOODS}

PG_COLUMN_SCHEMA = (
    "house_id TEXT, neighborhood TEXT, price INTEGER, square_feet INTEGER, "
    "num_bedrooms INTEGER, num_bathrooms INTEGER, house_age INTEGER, "
    "garage_spaces INTEGER, lot_size_acres NUMERIC(6,2), has_pool BOOLEAN, "
    "recently_renovated BOOLEAN, energy_rating TEXT, location_score INTEGER, "
    "school_rating INTEGER, crime_rate INTEGER, "
    "distance_downtown_miles NUMERIC(6,2), sale_date DATE, days_on_market INTEGER"
)


def extract(spark: SparkSession, csv_path: str) -> DataFrame:
    """Load the CSV dataset into a PySpark DataFrame with correct data types."""
    # Read the CSV file into a Spark DataFrame.
    # header=True means the first row contains column names.
    # inferSchema=True lets Spark guess data types like int, boolean, etc.
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
    )

    # Convert sale_date from strings like 1/3/22 into a real date column.
    # The dataset uses month/day/two-digit-year format.
    df = df.withColumn("sale_date", F.to_date(F.col("sale_date"), "M/d/yy"))

    return df

def transform(df: DataFrame) -> dict[str, DataFrame]:
    """Split the data by neighborhood and save each as a separate CSV file."""
    partitions = {}
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for hood in NEIGHBORHOODS:
        # Keep the original typed DataFrame for the PostgreSQL load step
        hood_df = df.filter(F.col("neighborhood") == hood).orderBy("house_id")
        partitions[hood] = hood_df

        # Create a separate DataFrame for CSV output so booleans match expected format
        csv_df = (
    hood_df
    .withColumn(
        "has_pool",
        F.when(F.col("has_pool"), F.lit("True")).otherwise(F.lit("False")),
    )
    .withColumn(
        "recently_renovated",
        F.when(
            F.col("recently_renovated"),
            F.lit("True"),
        ).otherwise(F.lit("False")),
    )
    .withColumn(
        "has_children",
        F.when(F.col("has_children"), F.lit("True")).otherwise(F.lit("False")),
    )
    .withColumn(
        "first_time_buyer",
        F.when(
            F.col("first_time_buyer"),
            F.lit("True"),
        ).otherwise(F.lit("False")),
    )
)

        temp_dir = OUTPUT_DIR / f"tmp_{hood.replace(' ', '_').lower()}"
        output_file = OUTPUT_FILES[hood]

        if temp_dir.exists():
            shutil.rmtree(temp_dir)

        if output_file.exists():
            output_file.unlink()

        (
            csv_df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(str(temp_dir))
        )

        part_file = next(temp_dir.glob("part-*.csv"))
        part_file.replace(output_file)
        shutil.rmtree(temp_dir)

    return partitions

def load(partitions: dict[str, DataFrame], jdbc_url: str, pg_props: dict) -> None:
    """Insert each neighborhood dataset into its own PostgreSQL table."""
    # Loop through each neighborhood and its DataFrame
    # from the dictionary returned by transform().
    for hood, hood_df in partitions.items():

        # Write the current neighborhood DataFrame into PostgreSQL using JDBC.
        (
            hood_df.write
            .format("jdbc")

            # PostgreSQL connection URL
            .option("url", jdbc_url)

            # Target table name for this neighborhood, for example public.downtown
            .option("dbtable", PG_TABLES[hood])

            # Database login information
            .option("user", pg_props["user"])
            .option("password", pg_props["password"])

            # PostgreSQL JDBC driver
            .option("driver", pg_props["driver"])

            # Explicit schema to use when creating the PostgreSQL table
            # Wrong code, do not use this line
            #.option("createTableColumnTypes", PG_COLUMN_SCHEMA)

            # Overwrite the table if it already exists from a previous run
            .mode("overwrite")
            .save()
        )


# ── Main (do not modify) ───────────────────────────────────────────────────────
def main() -> None:
    load_dotenv(ROOT / ".env")

    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('PG_HOST', 'localhost')}:"
        f"{os.getenv('PG_PORT', '5432')}/{os.environ['PG_DATABASE']}"
    )
    pg_props = {
        "user":     os.environ["PG_USER"],
        "password": os.getenv("PG_PASSWORD", ""),
        "driver":   "org.postgresql.Driver",
    }
    csv_path = str(ROOT / os.getenv("DATASET_DIR", "dataset") / os.getenv("DATASET_FILE", "historical_purchases.csv"))

    spark = (
        SparkSession.builder.appName("HouseSaleETL")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df         = extract(spark, csv_path)
    partitions = transform(df)
    load(partitions, jdbc_url, pg_props)

    spark.stop()


if __name__ == "__main__":
    main()
