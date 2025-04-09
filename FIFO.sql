
 WITH t as(
	SELECT 'A' prod, 100 qty, 'in' typ,  EOMONTH('2024-01-01') time UNION ALL
	SELECT 'A', 20, 'out', EOMONTH('2024-02-01') UNION ALL
    SELECT 'A', 10, 'in', EOMONTH('2024-03-01') UNION ALL
    SELECT 'A', 40, 'out', EOMONTH('2024-04-01') UNION ALL
    SELECT 'A', 30, 'out', EOMONTH('2024-05-01') UNION ALL
    SELECT 'A', 9, 'out', EOMONTH('2024-06-01') UNION ALL
    SELECT 'A', 11, 'out', EOMONTH('2024-07-01') UNION ALL
    SELECT 'A', 30, 'in', EOMONTH('2024-08-01') UNION ALL
    SELECT 'A', 3, 'in', EOMONTH('2024-09-01') UNION ALL
    SELECT 'A', 15, 'out', EOMONTH('2024-10-01') UNION ALL
    SELECT 'A', 12, 'out', EOMONTH('2024-11-01') UNION ALL
    SELECT 'A', 3, 'in', EOMONTH('2024-12-01') UNION ALL
    SELECT 'B', 16, 'in', EOMONTH('2024-01-01') UNION ALL
    SELECT 'B', 15, 'out', EOMONTH('2024-02-01') UNION ALL
    SELECT 'B', 14, 'in', EOMONTH('2024-03-01') UNION ALL
    SELECT 'B', 3, 'out', EOMONTH('2024-04-01') UNION ALL
    SELECT 'B', 10, 'out', EOMONTH('2024-05-01') UNION ALL
    SELECT 'B', 18, 'in', EOMONTH('2024-06-01') UNION ALL
    SELECT 'B', 17, 'out', EOMONTH('2024-07-01') UNION ALL
    SELECT 'B', 5, 'in', EOMONTH('2024-08-01') UNION ALL
    SELECT 'B', 3, 'in', EOMONTH('2024-09-01') UNION ALL
    SELECT 'B', 2, 'out', EOMONTH('2024-10-01') UNION ALL
    SELECT 'B', 5, 'in', EOMONTH('2024-11-01') UNION ALL
    SELECT 'B', 5, 'in', EOMONTH('2024-12-01')

),
FIFO AS 
	(
	SELECT
		prod, time,
		SUM(CASE WHEN typ='in' THEN qty ELSE -qty END)
		OVER (PARTITION BY prod ORDER BY time) AS rest_number,
		typ,
		qty
	FROM t
	)
	SELECT 
    prod,
    SUM(CASE WHEN typ = 'in' THEN qty ELSE 0 END) AS total_in,
    SUM(CASE WHEN typ = 'out' THEN qty ELSE 0 END) AS total_out,
    SUM(CASE WHEN typ = 'in' THEN qty ELSE -qty END) AS total_sum
FROM t
GROUP BY prod
ORDER BY prod;







