-- Silver layer: union teams from both APIs
CREATE OR REPLACE TABLE `voltaic-tooling-471807-t5.teams.standings` AS
SELECT leauge_id, team_id, overall_league_position FROM `voltaic-tooling-471807-t5.teams.standings_API2`
