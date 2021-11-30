SELECT
  aws.date,
  twt.country_code,
  aws.country,
  aws.continent,
  twt.tweet_counts,
  aws.total_deaths,
  aws.new_deaths,
  aws.new_cases,
  aws.new_cases_per_million
FROM
  `de-assignment-2.de_assignment_2.covid19_aws` aws
JOIN (
  SELECT
    country_code,
    date,
    COUNT(*) AS tweet_counts
  FROM
    `de-assignment-2.de_assignment_2.covid19_twitter`
  WHERE
    country IS NOT NULL
  GROUP BY
    1,
    2) twt
ON
  aws.country_code = twt.country_code
  AND aws.date = twt.date
ORDER BY
  aws.date
LIMIT
  100
