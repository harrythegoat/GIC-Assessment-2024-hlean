CREATE VIEW TopPerformingFundsMonthly AS
	with GetMonthYear AS (
		SELECT
			"fund",
			"symbol",
			"realised_pl",
			"datetime",
			TO_CHAR("datetime", 'MM-YYYY') AS "month_year"
		FROM funds_report WHERE "financial_type"='Equities'
	), PartitionByFund AS (
		SELECT
			"fund",
			"symbol",
			"datetime",
			"month_year",
			"realised_pl",
			 ROW_NUMBER() OVER
			(PARTITION BY "month_year", "fund")
		FROM GetMonthYear
	), GetMonthlyPL AS (
		SELECT
			"fund",
			"datetime",
			SUM("realised_pl") AS "total_pl",
			ROW_NUMBER() OVER
			(PARTITION BY "datetime")
		FROM PartitionByFund
		GROUP BY "datetime", "fund"
	), RankMonthlyPL AS (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY "datetime" ORDER BY MAX("total_pl")) as "rank",
			"datetime",
			"fund",
			"total_pl"
		FROM GetMonthlyPL
		GROUP BY "datetime", "fund", "total_pl"
	) SELECT "rank", "datetime", "fund", ROUND(CAST("total_pl" AS numeric), 2) AS "total_pl" FROM RankMonthlyPL WHERE "rank" = 10;

CREATE VIEW AllTimeTopPerformingFund AS SELECT "fund", COUNT("fund") AS "ranking" FROM TopPerformingFundsMonthly GROUP BY "fund" ORDER BY "ranking" DESC LIMIT 1;
