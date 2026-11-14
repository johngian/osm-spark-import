"""OSM import pipeline with Spark.

pyosmium decodes OSM data in Spark tasks; Spark keeps the cache as Iceberg
tables and does the joins; the mapping package turns elements into
table rows; batches of Parquet files are streamed into PostgreSQL by
the loader. See docs/design.md.
"""
