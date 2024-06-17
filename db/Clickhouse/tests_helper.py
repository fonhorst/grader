from .helper import stripe, create_table_query

TEST_TABLE = "test_trace"
TEST_CUBE_UID = "00000000-0000-0000-0000-000000000000"
TEST_CUBE_XLINE_SIZE = 600
TEST_CUBE_INLINE_SIZE = 600
TEST_CUBE_TRACE_SIZE = stripe

cube_db_exists = "SELECT * FROM system.tables WHERE database = 'cube'"

info_table_exists = (
    "SELECT * FROM system.tables WHERE database = 'cube' AND name = 'info'"
)

info_table_has_test_cube_id = (
    f"SELECT * FROM cube.info WHERE cube_uid = '{TEST_CUBE_UID}'"
)

insert_test_cube_id_into_info_table = (
    f"INSERT INTO cube.info (*) VALUES ('{TEST_CUBE_UID}', '{TEST_TABLE}')"
)

test_trace_table_exists = (
    f"SELECT * FROM system.tables WHERE database = 'cube' AND name = '{TEST_TABLE}'"
)

create_test_trace_table = create_table_query.replace("trace", TEST_TABLE)

test_trace_table_data = f"SELECT * FROM cube.{TEST_TABLE} LIMIT 1"

insert_parquet_into_test_trace_table = (
    f"INSERT INTO cube.{TEST_TABLE} (XLINE, INLINE, TRACE1, TRACE2) FORMAT Parquet"
)

insert_into_test_trace_table = (
    f"INSERT INTO cube.{TEST_TABLE} (XLINE, INLINE, TRACE1, TRACE2) VALUES"
)
