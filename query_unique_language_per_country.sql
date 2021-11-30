SELECT
  country_code,
  COUNT(DISTINCT(language_code)) AS unique_languages
FROM
  `de-assignment-2.de_assignment_2.covid19_twitter`
WHERE
  country_code != "NULL"
  AND country_code IS NOT NULL
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  1000
