# Viewfinder Panoramas STRM Data

This project converst the SRTM data available from http://viewfinderpanoramas.org/dem3.html into a parquet file for use in spark.

The full conversion requires Python 3.5+, NumPY, PySpark and several terabytes of temporary space. Once convereted to parquet, the resulting file should be ~35 GB.