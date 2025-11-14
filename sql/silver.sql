-- Silver layer: union teams from both APIs
CREATE OR REPLACE TABLE `voltaic-tooling-471807-t5.dataset.teams_silver` AS
SELECT * FROM `voltaic-tooling-471807-t5.dataset.teams_api1`
-- The first API not working - just example to union and after that i need to aggregate
-- UNION ALL
-- SELECT * FROM `voltaic-tooling-471807-t5.dataset.teams_api2`;

-- Silver layer: union standings from both APIs
CREATE OR REPLACE TABLE `voltaic-tooling-471807-t5.dataset.standings_silver` AS
SELECT * FROM `voltaic-tooling-471807-t5.dataset.standings_api1`
-- UNION ALL
--SELECT * FROM `voltaic-tooling-471807-t5.dataset.standings_api2`;
