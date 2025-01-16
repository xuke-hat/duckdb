import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from time import time_ns
import duckdb

t = pa.table([np.array([20000] * 1_000_000, dtype="datetime64[D]")], ["date"])
pq.write_table(t, "duckdb.parquet")

rel = duckdb.read_parquet(["duckdb.parquet"] * 50)
def run(times):
    q = "SELECT date FROM rel WHERE date=='2024-10-04'"
    
    if times == 4:
        res = duckdb.execute(q)
    else:
        res = duckdb.query(q)
        for _ in range(times):
            res = res.execute()
    return res.arrow()
    
for times in range(5):
    print(times)
    t = time_ns()     
    for i in range(5):
        run(times)
    print((time_ns() - t) / 1_000_000, "ms")
