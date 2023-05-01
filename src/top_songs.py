import os
from  pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StringType, TimestampType
from pyspark.sql.window import Window
from typing import Set


def extract_top_10_tracks(sc: SparkSession, input_file: str, output_file: str) -> None:
    df = read_data(sc, input_file)
    session_df = get_df_with_generated_sessions(df)
    top_50_sessions = get_top_50_longest_sessions(session_df)
    top_10_tracks = get_top_10_tracks(session_df, top_50_sessions)
    write_df_to_tsv(top_10_tracks, output_file)


def read_data (sc: SparkSession, input_file: str) -> DataFrame:
    schema = (StructType() 
          .add("user_id",StringType(),True) 
          .add("timestamp",TimestampType(),True) 
          .add("artist_id",StringType(),True)
          .add("artist_name",StringType(),True)
          .add("track_id",StringType(),True) 
          .add("track_name",StringType(),True) 
              )
    df = sc.read.csv(input_file, sep=r'\t', header=False, schema=schema)
    return df

def get_df_with_generated_sessions(df: DataFrame) -> DataFrame:
    windowSpec  = Window.partitionBy("user_id").orderBy("timestamp")  
    df = df.withColumn("prev_timestamp", F.lag("timestamp",1).over(windowSpec))
    # df.cache()
    df = df.withColumn("time_interval", (F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long"))/60)
    session_df = df.withColumn("session_start", F.when((F.col("time_interval") > 20) | (F.col("time_interval").isNull()), 1).otherwise(0))
    session_df = session_df.withColumn("user_session_id", F.sum("session_start").over(windowSpec))
    session_df = session_df.withColumn("global_session_id", F.concat_ws("_", "user_id", "user_session_id"))
    return session_df

def get_top_50_longest_sessions(session_df: DataFrame) -> Set[str]:
    top_50_sessions_df = session_df.groupBy("global_session_id").count().orderBy(F.col("count").desc()).limit(50)
    top_50_sessions = set((row.global_session_id for row in top_50_sessions_df.collect()))
    return top_50_sessions

def get_top_10_tracks(session_df: DataFrame, top_sessions: Set[str]) -> DataFrame:
    tracks_in_top_sessions = session_df.where(F.col("global_session_id").isin(top_sessions))
    top_10_tracks = tracks_in_top_sessions.groupBy("artist_name", "track_name", "track_id").count().orderBy(F.col("count").desc()).limit(10)
    return top_10_tracks

def write_df_to_tsv(df: DataFrame, output_path: str) -> None:
    df.write.option("delimiter", "\t").option("header", True).mode("overwrite").csv(output_path)