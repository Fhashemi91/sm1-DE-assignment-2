SELECT 
    continent, 
    date, 
    count(*) as num_of_countries,
    avg(cast(new_cases as FLOAT64)) as avg_new_cases,
    avg(cast(total_deaths as FLOAT64)) as avg_total_death,
    avg(cast(total_tests as FLOAT64)) as avg_total_death
FROM `de-assignment-2.de_assignment_2.covid19_aws` 
where continent is not null
group by 1, 2
order by continent, date
LIMIT 1000
