CREATE VIEW TopPerformingFundsMonthly AS
	with GetMonthYear AS (
		SELECT
			"FUND",
			"SYMBOL",
			"REALISED P/L",
			"DATE",
			TO_DATE(TO_CHAR("DATE", 'MM-YYYY'), 'MM-YYYY') AS "MONTHYEAR"
		FROM external_funds WHERE "FINANCIAL TYPE"='Equities'
	), PartitionByFund AS (
		SELECT
			"FUND",
			"SYMBOL",
			"DATE",
			"MONTHYEAR",
			"REALISED P/L",
			 ROW_NUMBER() OVER
			(PARTITION BY "MONTHYEAR", "FUND")
		FROM GetMonthYear
	), GetMonthlyPL AS (
		SELECT
			"FUND",
			"DATE",
			SUM("REALISED P/L") AS "TOTAL P/L",
			ROW_NUMBER() OVER
			(PARTITION BY "DATE")
		FROM PartitionByFund
		GROUP BY "DATE", "FUND"
	), RankMonthlyPL AS (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY "DATE" ORDER BY MAX("TOTAL P/L")) as "RANK",
			"DATE",
			"FUND",
			"TOTAL P/L"
		FROM GetMonthlyPL
		GROUP BY "DATE", "FUND", "TOTAL P/L"
	) SELECT "RANK", "DATE", "FUND", ROUND(CAST("TOTAL P/L" AS numeric), 2) AS "TOTAL P/L" FROM RankMonthlyPL WHERE "RANK" = 10;

CREATE VIEW AllTimeTopPerformingFund AS SELECT "FUND", COUNT("FUND") AS "RANKING" FROM TopPerformingFundsMonthly GROUP BY "FUND" ORDER BY "RANKING" DESC LIMIT 1;

CREATE VIEW WorstPerformingFundsMonthly AS
	with GetMonthYear AS (
		SELECT
			"FUND",
			"SYMBOL",
			"REALISED P/L",
			"DATE",
			TO_DATE(TO_CHAR("DATE", 'MM-YYYY'), 'MM-YYYY') AS "MONTHYEAR"
		FROM external_funds WHERE "FINANCIAL TYPE"='Equities'
	), PartitionByFund AS (
		SELECT
			"FUND",
			"SYMBOL",
			"DATE",
			"MONTHYEAR",
			"REALISED P/L",
			 ROW_NUMBER() OVER
			(PARTITION BY "MONTHYEAR", "FUND")
		FROM GetMonthYear
	), GetMonthlyPL AS (
		SELECT
			"FUND",
			"DATE",
			SUM("REALISED P/L") AS "TOTAL P/L",
			ROW_NUMBER() OVER
			(PARTITION BY "DATE")
		FROM PartitionByFund
		GROUP BY "DATE", "FUND"
	), RankMonthlyPL AS (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY "DATE" ORDER BY MAX("TOTAL P/L")) as "RANK",
			"DATE",
			"FUND",
			"TOTAL P/L"
		FROM GetMonthlyPL
		GROUP BY "DATE", "FUND", "TOTAL P/L"
	) SELECT "RANK", "DATE", "FUND", ROUND(CAST("TOTAL P/L" AS numeric), 2) AS "TOTAL P/L" FROM RankMonthlyPL WHERE "RANK" = 1;
CREATE VIEW AllTimeWorstPerformingFund AS SELECT "FUND", COUNT("FUND") AS "RANKING" FROM WorstPerformingFundsMonthly GROUP BY "FUND" ORDER BY "RANKING" DESC LIMIT 1;

SELECT * FROM TopPerformingFundsMonthly;
SELECT * FROM AllTimeTopPerformingFund;
SELECT * FROM WorstPerformingFundsMonthly;
SELECT * FROM AllTimeWorstPerformingFund;

SELECT 
	tpf."DATE", 
	tpf."FUND" as "TOP PERFORMING FUND", 
	tpf."TOTAL P/L", 
	wpf."FUND" as "LOWER PERFORMING FUND", 
	wpf."TOTAL P/L"
FROM TopPerformingFundsMonthly as tpf
INNER JOIN WorstPerformingFundsMonthly as wpf
ON tpf."DATE" = wpf."DATE";
