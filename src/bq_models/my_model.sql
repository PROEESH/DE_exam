CREATE OR REPLACE TABLE `voltaic-tooling-471807-t5.teams.dim_country_summary` AS
SELECT
  COUNT(*) AS total_teams
FROM `voltaic-tooling-471807-t5.teams.teams`