-- DROP DATABASE IF EXISTS cube;
CREATE DATABASE IF NOT EXISTS cube;

CREATE TABLE IF NOT EXISTS cube.trace
(
    XLINE  UInt32,
    INLINE UInt32,
    TRACE1 Array(Float64),
    TRACE2 Array(Float64),
    TRACE3 Array(Float64),
    TRACE4 Array(Float64),
    TRACE5 Array(Float64),
    TRACE6 Array(Float64),
    TRACE7 Array(Float64),
    TRACE8 Array(Float64)
) ENGINE = MergeTree() PRIMARY KEY (INLINE, XLINE) ORDER BY tuple(INLINE, XLINE);

CREATE TABLE IF NOT EXISTS cube.info
(
    cube_uid   UUID,
    table_name CHAR(64)
) ENGINE = MergeTree() PRIMARY KEY (cube_uid) ORDER BY cube_uid;

CREATE DATABASE IF NOT EXISTS volume;

CREATE TABLE IF NOT EXISTS volume.barchart
(
    volume_uuid   UUID,
    min_amplitude Float32,
    max_amplitude Float32,
    step          Float32,
    bar_values Array(Int32)
) ENGINE = MergeTree() PRIMARY KEY (volume_uuid);