import math

max_trace_length = 1501
stripe = 32

num_columns = math.ceil(max_trace_length / stripe)

columns = ", ".join([f"TRACE{i + 1} Array(Float64)" for i in range(num_columns)])
colum_names = ", ".join([f"TRACE{i + 1}" for i in range(num_columns)])
traces = ", ".join(
    [
        f"arraySlice(FINAL_TRACE, {i * stripe + 1}, {stripe}) as TRACE{i + 1}"
        for i in range(num_columns)
    ]
)
query = f"INSERT INTO cube.trace SELECT INLINE, XLINE, {traces} FROM orig_trace"

create_table_query = f"CREATE TABLE cube.trace (INLINE UInt32, XLINE UInt32, {columns}) ENGINE = MergeTree() PRIMARY KEY(INLINE,XLINE) ORDER BY tuple(INLINE,XLINE);"

create_table_cube_info_query = "CREATE TABLE cube.info (cube_uid CHAR(64), table_name CHAR(64)) ENGINE = MergeTree() PRIMARY KEY(cube_uid) ORDER BY cube_uid;"

cube_info_insert_query = "INSERT INTO cube.info (*) VALUES ('sum_cube_uid', 'trace')"

select_query = (
    f"SELECT INLINE, XLINE, arrayJoin(arrayZip(arr_res, arrayEnumerate(arr_res))) as aj "
    f"FROM (SELECT INLINE, XLINE, arrayConcat({colum_names}) as arr_res FROM trace "
    f"WHERE INLINE = 1500 and XLINE >= 3892 and XLINE <= 6119) "
    f"INTO OUTFILE '/tmp/file42.data' FORMAT Native"
)
