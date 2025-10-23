import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
from urllib.parse import urlparse, urlunparse
load_dotenv()


def make_jdbc_url(db_url):
    """
    Converts a normal Postgres URL into a JDBC URL compatible with Spark,
    and extracts username/password from the URL.
    args:
     db_url: str : Standard Postgres connection URL
    returns:
     tuple : (jdbc_url: str, username: str, password: str)
    """
    parsed = urlparse(db_url)

    netloc = parsed.hostname
    if parsed.port:
        netloc += f":{parsed.port}"

    query = parsed.query

    jdbc_url = urlunparse(("jdbc:postgresql", netloc, parsed.path, "", query, ""))
    return jdbc_url, parsed.username, parsed.password



def get_spark_connection(env: str = os.getenv("ENV", "test")):
    """Create Spark session configured for local or AWS S3 depending on env."""
    spark = (
        SparkSession.builder
        .appName(f"YT_Pyspark_{env}")
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.6.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.getenv('AWS_REGION')}.amazonaws.com")
        .getOrCreate()
    )
    return spark

def load_categorical_data(spark, jdbc_url, props, env: str = os.getenv("ENV")):
    """
    Load the categorical_data table from the appropriate schema into a Spark DataFrame.
    args:
     env: str : 'test' or 'prod' to determine which schema to use
     spark: SparkSession : Active Spark session
     jdbc_url: str : JDBC URL for database connection
     props: dict : Connection properties including user, password, driver
     """
    schema = "aq_test_local" if env == "test" else "yt_data"
    table_name = f"{schema}.categorical_data"
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=props)
    return df

def get_output_path(env: str = os.getenv("ENV")): 
    """Returns the S3 output path based on environment.
    arg:
    env: str : 'test' or 'prod' to determine output path
    """
    
    if env == "test":
        return "s3a://yt-pyspark/pipeline/test"
    return "s3a://yt-pyspark/pipeline/prod"

def run_spark_job(env = os.getenv("ENV", "test")):
    """Main function to run the Spark ETL job and store pyspark parquets to a s3 bucket
    for analysis and reading.
    arg:
    env: str : 'test' or 'prod' to determine configurations
    returns:
    dict : Status of the job and output path if successful
    """
    try:
        print(f"\n\033[34mRunning Spark job in [{env.upper()}] mode\033[0m\n")

        spark = get_spark_connection(env)
        output_path = get_output_path(env)


        if env == "test":
            # Localhost test
            jdbc_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'postgres')}"
            props = {
                "user": os.getenv("POSTGRES_USER"),
                "password": os.getenv("POSTGRES_PASSWORD"),
                "driver": "org.postgresql.Driver"
            }
            trending_table = "aq_test_local.youtube_trending_history_p"
            videos_table = "aq_test_local.youtube_videos_p"
            
        else:
            # Production NeonDB
            db_url = os.getenv("DB_URL")
            jdbc_url, db_user, db_pw = make_jdbc_url(db_url)

            props = {
                "user": os.getenv("PROD_USER") or db_user,
                "password": os.getenv("PROD_PW") or db_pw,
                "driver": "org.postgresql.Driver"
            }
            trending_table = "yt_data.trending_history"
            videos_table = "yt_data.youtube_videos"

        # --- Determine current week range ---
        today = datetime.now().date()
        monday = today - timedelta(days=today.weekday())
        next_monday = monday + timedelta(days=7)
        print(f"\033[33mReading data between {monday} and {next_monday}\033[0m")

        # --- Read trending_history ---
        trending_df = (spark.read.jdbc(url=jdbc_url, table=trending_table, properties=props)
                        .filter((col("recorded_at") >= monday) & (col("recorded_at") < next_monday)))

        # --- Read videos ---
        videos_df = spark.read.jdbc(url=jdbc_url, table=videos_table, properties=props).select("video_id", "category_id")

        # --- Join trending with videos ---
        trending_df = trending_df.join(videos_df, on="video_id", how="left").cache()

        if trending_df.count() == 0:
            print("\033[31mWARNING! No data for this week, skipping write.\033[0m")
            spark.stop()
            return

        # --- Load categorical data table dynamically ---
        category_df = load_categorical_data(env, spark, jdbc_url, props)

        category_df = category_df.withColumnRenamed("id", "category_id")


        # --- Join to get category names ---
        trending_df = trending_df.join(category_df, on="category_id", how="left")


        # --- Write weekly Parquet ---
        week_str = monday.strftime("%Y_%m_%d")
        output_dir = f"{output_path}/week_{week_str}"
        columns_to_keep = trending_df.columns  
        columns_to_keep.remove("category_id") 
        trending_df.select(columns_to_keep).write.mode("overwrite").parquet(output_dir)

        print(f"\n\033[1;32mSuccessfully wrote Parquet to: {output_dir}\033[0m\n")

        spark.stop()

        return {"status": "success", "output_path": output_dir}

    except Exception as e:
        print(f"\033[1;31mERROR: Spark job failed: {e}\033[0m") 
        return {"status": "failure"}
        

# run_spark_job()