SELECT
  continent,
  date,
  COUNT(*) AS num_of_countries,
  AVG(CAST(new_cases AS FLOAT64)) AS avg_new_cases,
  AVG(CAST(total_deaths AS FLOAT64)) AS avg_total_death,
  AVG(CAST(total_tests AS FLOAT64)) AS avg_total_tests
FROM
  `de-assignment-2.de_assignment_2.covid19_aws`
WHERE
  continent IS NOT NULL
GROUP BY
  1,
  2
ORDER BY
  continent,
  date
LIMIT
  1000
