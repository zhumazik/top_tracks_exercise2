from  pyspark.sql import SparkSession
from top_songs import *


data_folder = 'mnt/'
input_fie = data_folder + 'userid-timestamp-artid-artname-traid-traname.tsv'
output_file = data_folder + 'result.tsv'


sc = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("home_task")\
    .getOrCreate()


extract_top_10_tracks(sc, input_fie, output_file)