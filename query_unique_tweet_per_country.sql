SELECT
  country_code,
  date,
  COUNT(DISTINCT(tweet_id)) AS unique_tweets
FROM
  `de-assignment-2.de_assignment_2.covid19_twitter`
WHERE
  country_code != "NULL"
  AND country_code IS NOT NULL
GROUP BY
  1,
  2
ORDER BY
  1,
  3 DESC
LIMIT
  1000
