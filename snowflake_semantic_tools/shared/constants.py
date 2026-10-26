"""Shared constants used across validation and generation."""

SQL_FUNCTION_KEYWORDS = frozenset({"CAST", "EXTRACT", "TRIM", "CONVERT", "DATE_TRUNC", "DATEADD", "DATEDIFF"})

# Common aliases for column_type values that users may write but SST does not accept.
# Used to generate "Did you mean?" suggestions in validation and parsing errors.
COLUMN_TYPE_ALIASES = {
    "measure": "fact",
    "metric": "fact",
    "timestamp": "time_dimension",
    "time": "time_dimension",
    "date": "time_dimension",
}
