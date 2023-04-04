import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, count, col, explode, lower, regexp_extract

spark = (SparkSession
  .builder
  .appName("ShrekWordCounter")
  .getOrCreate())

shrek_script_df = (spark.read.format("text")
    .option("header", "false")
    .option("inferSchema", "true")
    .load("./shrek_script.txt")
    )

shrek_script_lines = shrek_script_df.select(split(col("value"), " ").alias("line"))
shrek_script_lines.show(10)

shrek_script_words = shrek_script_lines.select(explode(col("line")).alias("word"))
shrek_script_words.show(15)

shrek_script_words_lower = shrek_script_words.select(lower(col("word")).alias("word_lower"))
shrek_script_words_lower.show(15)

shrek_script_words_clean = shrek_script_words_lower.select(
    regexp_extract(col("word_lower"), "[a-z]*", 0).alias("word")
)
shrek_script_words_clean.show(20)

shrek_script_words_nonull = shrek_script_words_clean.where(col("word") != "")
shrek_script_words_nonull.show()

shrek_script_groups = shrek_script_words_nonull.groupBy(col("word"))
shrek_script_results = shrek_script_groups.count()
shrek_script_results.orderBy("count", ascending=False).show(100)

shrek_script_results.coalesce(1).write.csv("./shrek_word_counts.csv")