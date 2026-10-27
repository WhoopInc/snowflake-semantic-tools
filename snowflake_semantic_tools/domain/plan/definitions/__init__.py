"""The definitions plan and diff compare, one module per artifact type, as `member_diff` compares them.

Each module builds a `Definition` from the desired side, what SST compiled, and one from what a
live read of the published object returned, with one member kind and property name for each
thing the two sides can both state.
"""
