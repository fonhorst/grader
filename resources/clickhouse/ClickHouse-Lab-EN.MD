# ClickHouse Lab

During this lab, you have to implement a Data Warehouse (DWH) using ClickHouse (CH) and its techniques, such as Materialized Views (MV) and Distributed tables. The maximum score for this lab is **20**.

## Dataset

The dataset is presented as a parquet file with users’ transactions. The path to this file:

```
/nfs/shared/clickhouse_data/transactions_12M.parquet
```

### Dataset Sample:

| user_id_out | user_id_in | important | amount |     datetime           |
|-------------|------------|-----------|--------|------------------------|
| 2781        | 3343       | 0         | 199.2  | 2018-09-02 17:25:12    |
| 2789        | 3343       | 0         | 566.33 | 2018-11-26 11:29:26    |
| 2838        | 3343       | 0         | 85.42  | 2018-09-05 19:59:22    |
| 2850        | 3343       | 0         | 850.74 | 2018-02-19 14:47:41    |
| 2860        | 3343       | 0         | 238.35 | 2018-10-16 00:58:21    |
| 2872        | 3343       | 0         | 940.16 | 2018-09-17 08:24:18    |
| 2874        | 3343       | 0         | 308    | 2018-12-13 11:59:50    |
| 2878        | 3343       | 0         | 709.32 | 2018-11-20 10:35:57    |
| 2891        | 3343       | 0         | 121.71 | 2018-11-27 03:59:52    |
| 2939        | 3343       | 0         | 240.06 | 2018-08-27 20:03:52    |


### Dataset Properties:

- ~20% of transactions are important (`important == 1`)
- Total record count: **12 million**

## Task

1. Choose **two or more** MVs from the list below.
2. Upload the data into the CH cluster.
   - The table for uploaded data must be of the **MergeTree** family.
   - Use the **Distributed engine** and an appropriate **sharding expression** to distribute data over the cluster.
3. Implement the chosen MVs.
   - You may create extra tables with different engines if needed.
   - The number of extra tables should be **reasonable**.

## Requirements

- **All tables (including MVs) must be sharded**.
  - Your database must not contain any table that keeps all the data from the cluster.
  - If using a View, it **must not** request all the data to aggregate it on the initiator server.
- **Table schemas should be reasonable** (e.g., defining a `String` column to store digits is a bad idea).
- **MVs must return correct results**.
- Users must be able to retrieve results from **any** server in the cluster.

## Example Queries

```sql
SELECT user_id, balance FROM users_balance WHERE balance < 0 LIMIT 20;
```

```sql
SELECT * FROM avg_out_month WHERE user_id = 12345;
```

```sql
SELECT user_id, max(trans_sum) FROM transactions_sum 
WHERE user_id IN (123, 456, 789, …) 
GROUP BY user_id;
```

## Materialized Views (MVs) Options

1. **Average amount** for incoming and outgoing transactions by **months and days** for each user.
2. **Number of important transactions** for incoming and outgoing transactions by **months and days** for each user.
3. **Sum** of incoming and outgoing transactions by **months** for each user.
4. **Users’ saldo** for the current moment.

## Passing Procedure

You must prepare a **short PDF report** containing:

- **Table schemas**
- **Justification** for the chosen sharding expression
- **List of chosen MVs** and their **creation queries**

The report should be saved in your NFS home directory:

```
/nfs/home/<login>/lab-clickhouse.pdf
```

## Deadline

📅 **10.06, 23:59**

⚠ **PLAGIARISM IS PUNISHABLE UP TO NON-ADMISSION TO THE EXAM!**

🚨 **DO THE TASKS ON YOUR OWN!**

