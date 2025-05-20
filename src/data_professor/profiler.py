from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, countDistinct, isnan

class Profiler:
    """
    Profiles a Spark DataFrame by computing, for each column:
      - data type
      - total rows (inferred)
      - null count
      - empty-string count (for string columns)
      - distinct count
      - numeric summaries (min, max, mean) for numeric columns
    """

    def profile(self, df: DataFrame) -> dict:
        """
        Analyze df and return a metadata dict of the form:
        {
          column_name: {
            "dtype": str,
            "null_count": int,
            "empty_count": int,        # only for string columns
            "distinct_count": int,
            "min": <numeric>,          # only for numeric columns
            "max": <numeric>,          # only for numeric columns
            "mean": <float>,           # only for numeric columns
          },
          ...
        }
        """
        metadata = {}
        total_rows = df.count()

        for name, dtype in df.dtypes:
            col_stats = {"dtype": dtype, "total_rows": total_rows}

            # Nulls
            col_stats["null_count"] = (
                df.filter(col(name).isNull() | when(isnan(col(name)), True)).count()
            )

            # Empty strings
            if dtype == "string":
                col_stats["empty_count"] = df.filter(col(name) == "").count()

            # Distinct values
            col_stats["distinct_count"] = df.select(countDistinct(col(name))).collect()[0][0]

            # Numeric summaries
            if dtype in ("int", "bigint", "double", "float", "long", "decimal"):
                summary = (
                    df.selectExpr(
                        f"min({name}) as min",
                        f"max({name}) as max",
                        f"avg({name}) as mean"
                    )
                    .collect()[0]
                )
                col_stats["min"] = summary["min"]
                col_stats["max"] = summary["max"]
                col_stats["mean"] = summary["mean"]

            metadata[name] = col_stats

        return metadata