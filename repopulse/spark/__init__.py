"""Bronze -> silver incremental batch jobs (PySpark + Delta Lake).

Kept light on purpose: importing this package, ``job``, ``pii`` or ``rules`` never imports
pyspark. The Spark code lives in ``repopulse.spark.batch`` and is imported only when a batch runs.
See docs/spark-jobs.md.
"""
