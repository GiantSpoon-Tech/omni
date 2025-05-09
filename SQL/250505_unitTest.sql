/* ================================================================
   COST‑MODEL v5  –  EXTENDED EDGE‑CASE UNIT TESTS
================================================================== */
DECLARE today DATE DEFAULT DATE '2025-05-05';

/*-----------------------------------------------------------------
  1)  MOCK DELIVERY  – edge‑cases included
-----------------------------------------------------------------*/
CREATE TEMP TABLE tmp_raw AS
SELECT * FROM UNNEST([
  /* Regular CPM standard + OOF row (pkg1) ― already tested earlier */
  STRUCT(
     DATE '2025-04-01'           AS date,
     'CPM_STD'                   AS campaign,
     'pkg1'                      AS package_id,
     'pl1'                       AS placement_id,
     1000                        AS impressions,
     0                           AS clicks,
     0                           AS total_conversions,
     'CPM'                       AS cost_method,
     5.0                         AS rate_raw,
     DATE '2025-04-01'           AS start_date,
     DATE '2025-04-02'           AS end_date,
     10000                       AS planned_imps,
     50.0                        AS planned_cost
  ),
  STRUCT(DATE '2025-04-03','CPM_STD','pkg1','pl1', 300,0,0,'CPM',5.0,
         DATE '2025-04-01',DATE '2025-04-02',10000,50.0),   -- OOF

  /* ───────── Zero‑impression CPM row (pkgZ) ───────── */
  STRUCT(DATE '2025-04-02','CPM_ZERO','pkgZ','plZ',0,0,0,'CPM',7.0,
         DATE '2025-04-01',DATE '2025-04-05',5000,35.0),

  /* ───────── Live‑flight Flat, no planned‑imps (pkgLive) ───────── */
  STRUCT(today        ,'FLAT_LIVE','pkgLive','pl1',600,0,0,'Flat',NULL,
         DATE '2025-05-01',DATE '2025-05-10',0,100.0),
  STRUCT(DATE_ADD(today, INTERVAL -1 DAY),'FLAT_LIVE','pkgLive','pl2',400,0,0,'Flat',NULL,
         DATE '2025-05-01',DATE '2025-05-10',0,100.0),

  /* ───────── Leap‑day CPM package (pkgLeap) ───────── */
  STRUCT(DATE '2024-02-29','CPM_LEAP','pkgLeap','pl1',800,0,0,'CPM',4.0,
         DATE '2024-02-28',DATE '2024-03-02',3000,12.0),

  /* ───────── Flat package with IF & OOF rows (pkgFIO) ───────── */
  STRUCT(DATE '2025-06-01','FIO','pkgFIO','pl1',500,0,0,'Flat',NULL,
         DATE '2025-06-01',DATE '2025-06-02',1000,50.0),   -- IF
  STRUCT(DATE '2025-06-05','FIO','pkgFIO','pl1',300,0,0,'Flat',NULL,
         DATE '2025-06-01',DATE '2025-06-02',1000,50.0),   -- OOF

  /* ───────── Underdelivery CPM (pkgU)  delivered < planned_imps ───────── */
  STRUCT(DATE '2025-04-01','CPM_UNDER','pkgU','pl1',200,0,0,'CPM',10.0,
         DATE '2025-04-01',DATE '2025-04-03',5000,50.0),

  /* ───────── Underdelivery Flat with planned‑imps (pkgUF) ───────── */
  STRUCT(DATE '2025-04-02','FLAT_UNDER','pkgUF','pl1',100,0,0,'Flat',NULL,
         DATE '2025-04-01',DATE '2025-04-02',1000,120.0)
]) AS t;

/*-----------------------------------------------------------------
  2)  MOCK PRISMA  (only necessary fields)
-----------------------------------------------------------------*/
CREATE TEMP TABLE tmp_prisma AS
SELECT DISTINCT
  package_id, cost_method, start_date, end_date,
  planned_imps AS planned_imps_pk,
  planned_cost AS planned_cost_pk
FROM tmp_raw;

/*-----------------------------------------------------------------
  3)  CALC TABLE  (join + inflight roll‑up)
-----------------------------------------------------------------*/
CREATE TEMP TABLE calc AS
WITH base AS (
  SELECT
    r.date, r.package_id, r.placement_id,
    r.impressions, r.clicks, r.total_conversions,
    p.cost_method                 AS p_cost_method,
    r.rate_raw,
    p.planned_imps_pk             AS p_pkg_total_planned_imps,
    p.planned_cost_pk             AS p_pkg_total_planned_cost,
    CASE WHEN r.date BETWEEN p.start_date AND p.end_date THEN 0 ELSE 1 END AS flight_date_flag
  FROM tmp_raw r
  JOIN tmp_prisma p USING(package_id)
),
roll AS (
  SELECT package_id,
         SUM(CASE WHEN TRUE THEN impressions END) AS total_inflight_impressions
  FROM base
  GROUP BY package_id
)
SELECT
  b.*,
  r.total_inflight_impressions,
  SAFE_DIVIDE(b.impressions,r.total_inflight_impressions) AS pkg_inflight_imps_perc
FROM base b
JOIN roll r USING(package_id);

/*-----------------------------------------------------------------
  4)  APPLY v5 PRICING LOGIC  (core rules only)
-----------------------------------------------------------------*/
CREATE TEMP TABLE result AS
SELECT
  *,
  CASE
    WHEN flight_date_flag = 1 THEN 0
    WHEN p_cost_method = 'CPM' THEN
         CASE
           WHEN total_inflight_impressions > p_pkg_total_planned_imps /* over‑delivery cap */
           THEN p_pkg_total_planned_cost * impressions / total_inflight_impressions
           ELSE rate_raw * impressions / 1000
         END
    WHEN p_cost_method = 'CPC' THEN rate_raw * clicks
    WHEN p_cost_method = 'CPA' THEN rate_raw * total_conversions
    WHEN p_cost_method = 'Flat' THEN
         CASE
           WHEN p_pkg_total_planned_imps = 0
           THEN p_pkg_total_planned_cost * pkg_inflight_imps_perc
           ELSE p_pkg_total_planned_cost *
                SAFE_DIVIDE(impressions,total_inflight_impressions)
         END
    ELSE 0
  END AS daily_cost
FROM calc;

/*-----------------------------------------------------------------
  5)  ASSERTIONS
-----------------------------------------------------------------*/
-- Zero‑impression CPM → cost 0
ASSERT (SELECT daily_cost FROM result WHERE package_id='pkgZ') = 0
  AS 'Zero‑impression CPM cost not zero';

-- Live‑flight Flat  (pkgLive)  today mid‑flight: prorated cost = 100 × (5 active /10) = 50
ASSERT (SELECT SUM(daily_cost) FROM result WHERE package_id='pkgLive') = 50
  AS 'Live‑flight Flat proration wrong';

-- Leap‑day CPM   800 imps × $4 /1000 = 3.2
ASSERT (SELECT daily_cost FROM result WHERE package_id='pkgLeap') = 3.2
  AS 'Leap‑day CPM cost wrong';

-- Flat IF vs OOF rows  (pkgFIO)  planned cost 50
--   IF row: 500/500 = 1   cost = 50
--   OOF row: cost = 0
ASSERT (SELECT daily_cost FROM result WHERE package_id='pkgFIO' AND flight_date_flag=0) = 50
  AS 'Flat IF row cost wrong';
ASSERT (SELECT daily_cost FROM result WHERE package_id='pkgFIO' AND flight_date_flag=1) = 0
  AS 'Flat OOF row cost not zero';

-- Underdelivery CPM (pkgU) 200 imps × $10 /1000 = 2
ASSERT (SELECT daily_cost FROM result WHERE package_id='pkgU') = 2
  AS 'Underdelivery CPM wrong';

-- Underdelivery Flat with planned‑imps (pkgUF)
--   Delivered 100 / Planned 1000 → cost = 120 × 100/100 = 12
ASSERT (SELECT daily_cost FROM result WHERE package_id='pkgUF') = 12
  AS 'Underdelivery Flat planned-imps wrong';

/*-----------------------------------------------------------------
  6)  SUCCESS OUTPUT
-----------------------------------------------------------------*/
SELECT '✅  ALL EDGE‑CASE TESTS PASSED' AS status;
