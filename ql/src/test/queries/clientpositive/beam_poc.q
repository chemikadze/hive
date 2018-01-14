set hive.cbo.enable=false;


set hive.execution.engine=mr;
EXPLAIN SELECT * FROM src WHERE key = "1";


set hive.execution.engine=mr;
EXPLAIN SELECT * FROM src WHERE key = "1";


set hive.execution.engine=beam;

EXPLAIN SELECT "${hiveconf:hive.execution.engine}";

EXPLAIN LOGICAL SELECT * FROM src LIMIT 1;

EXPLAIN SELECT * FROM src LIMIT 1;

-- SELECT * FROM src LIMIT 1;

EXPLAIN LOGICAL SELECT * FROM src WHERE key = "1";

EXPLAIN SELECT * FROM src WHERE key = "1";

SELECT * FROM src WHERE key = "1";