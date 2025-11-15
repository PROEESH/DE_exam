SELECT * FROM `voltaic-tooling-471807-t5.teams.teams_API2`  teams
LEFT OUTER JOIN `voltaic-tooling-471807-t5.teams.standings_API2` standings
on teams.team_id = standings.team_id