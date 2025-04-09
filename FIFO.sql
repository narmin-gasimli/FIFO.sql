-- task
-- A və B məhsuli, aylar üzrə nə qədər köhnəlib onu tapmaq lazımdır FİFO(ilk girən ilk çıxar) yanaşmasi ilə.
-- hesabat tarixi 2025-01-01 dir.
---  misal mən 2024 yanvarda, 50 karandaş aldım sonra 2024 fevralda 20 dənəsini aldım. sonra martda 40 dənə satdım. bu 40 dənə 50 dənə karandaşdan azalır. dekabrda 15 dənə yenə karandaş aldım.
--  anbarda 45 karandaş var. hesabatımda, 0-1 ay, 1-2 ay, 2-3 ay, ....11-12 ay, sütunlarım olacaq.  0-1 ayda=15, 10-11 ayda= 20, 11-12 aya=10 digərləri 0 göstərməlidir.



--	SELECT 
--		prod,
--		SUM(CASE WHEN time = EOMONTH('2024-01-01') AND typ = 'in' THEN qty ELSE 0 END) AS January_IN,
--		SUM(CASE WHEN time = EOMONTH('2024-01-01') AND typ = 'out' THEN qty ELSE 0 END) AS January_OUT,
--		MAX(CASE WHEN time = EOMONTH('2024-01-01') THEN rest_number ELSE 0 END) AS January_rest,
--		SUM(CASE WHEN time = EOMONTH('2024-02-01') AND typ = 'in' THEN qty ELSE 0 END) AS February_IN,
--		SUM(CASE WHEN time = EOMONTH('2024-02-01') AND typ = 'out' THEN qty ELSE 0 END) AS February_OUT,
--		MAX(CASE WHEN time = EOMONTH('2024-02-01') THEN rest_number ELSE 0 END) AS February_rest,
--		SUM(CASE WHEN time = EOMONTH('2024-03-01') AND typ = 'in' THEN qty ELSE 0 END) AS March_IN,
--		SUM(CASE WHEN time = EOMONTH('2024-03-01') AND typ = 'out' THEN qty ELSE 0 END) AS March_OUT,
--		MAX(CASE WHEN time = EOMONTH('2024-03-01') THEN rest_number ELSE 0 END) AS March_rest
--	FROM FIFO
--	WHERE time IN ( EOMONTH('2024-01-01'), EOMONTH('2024-02-01'), EOMONTH('2024-03-01') )
--GROUP BY prod;






--SELECT
--		prod,
--    SUM(CASE WHEN DATEDIFF(MONTH, time, '2025-02-28') = 0 THEN qty ELSE 0 END) AS "Less than a month",
--    SUM(CASE WHEN DATEDIFF(MONTH, time, '2025-02-28') BETWEEN 1 AND 2 THEN qty ELSE 0 END) AS "1-2 months",
--    SUM(CASE WHEN DATEDIFF(MONTH, time, '2025-02-28') BETWEEN 3 AND 4 THEN qty ELSE 0 END) AS "3-4 months",
--    SUM(CASE WHEN DATEDIFF(MONTH, time, '2025-02-28') BETWEEN 5 AND 6 THEN qty ELSE 0 END) AS "5-6 months",
--    SUM(CASE WHEN DATEDIFF(MONTH, time, '2025-02-28') BETWEEN 7 AND 8 THEN qty ELSE 0 END) AS "7-8 months",
--    SUM(CASE WHEN DATEDIFF(MONTH, time, '2025-02-28') BETWEEN 9 AND 10 THEN qty ELSE 0 END) AS "9-10 months",
--    SUM(CASE WHEN DATEDIFF(MONTH, time, '2025-02-28') BETWEEN 11 AND 12 THEN qty ELSE 0 END) AS "11-12 months",
--    SUM(CASE WHEN DATEDIFF(DAY, time, '2025-02-28') > 360 THEN qty ELSE 0 END) AS "360+",
--    SUM(qty) AS "Total"
--FROM FIFO
--WHERE typ = 'in'
--GROUP BY prod
--ORDER BY prod;


--SELECT * FROM FIFO;









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







