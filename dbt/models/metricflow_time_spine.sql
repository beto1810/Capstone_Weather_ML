SELECT
    DATEADD(day, seq4(), '2020-01-01') AS date_day
FROM TABLE(GENERATOR(ROWCOUNT => 5000))  -- generates ~13 years of dates