"""OSM import pipeline on Hadoop/HDFS.

pyosmium decodes OSM data in Spark tasks; Spark keeps the cache as Iceberg
tables on HDFS and does the joins; the mapping package turns elements into
table rows; batches of Parquet files on HDFS are streamed into PostgreSQL by
the loader. See docs/design.md.
"""
