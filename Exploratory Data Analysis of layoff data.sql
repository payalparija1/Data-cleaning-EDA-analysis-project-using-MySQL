SELECT * 
FROM world_layoffs.layoffs_staging2;

SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2;

SELECT * 
FROM world_layoffs.layoffs_staging2
where total_laid_off = 'NULL';

SELECT distinct total_laid_off
from world_layoffs.layoffs_staging2
order by 1;

SELECT distinct percentage_laid_off
from world_layoffs.layoffs_staging2
order by 1;

-- Before doing Exploratory data analysys we need to change the data type of nymerical fields like total_laid_off, percentage_laid_off to integer
-- We need to change text 'NULL' to 0


Update world_layoffs.layoffs_staging2
set total_laid_off = 0
where total_laid_off = 'NULL';

Update world_layoffs.layoffs_staging2
set percentage_laid_off = 0
where percentage_laid_off = 'NULL';

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN total_laid_off INT;

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `percentage_laid_off` DECIMAL(6,5) NOT NULL DEFAULT '0.0';


select MAX(total_laid_off), max(percentage_laid_off )
from world_layoffs.layoffs_staging2;

select count(*) 
from world_layoffs.layoffs_staging2
where percentage_laid_off = 1; 
-- 116 companies laid off their total work force

select * 
from world_layoffs.layoffs_staging2
where percentage_laid_off = 1
order by  total_laid_off desc;
-- Katerra a USA based construction company, removed all of its employees and became bankcrupt after 2022
-- https://en.wikipedia.org/wiki/Katerra

select company, sum(total_laid_off)
from world_layoffs.layoffs_staging2
group by company
order by 2 desc;
-- Amazon, Google, Meta, Salesforce, Microsoft are the major company involved in maximum layoffs.

select industry, sum(total_laid_off)
from world_layoffs.layoffs_staging2
group by industry
order by 2 desc;
-- consumer and Retail industry is the most affected by the Covid Pandemic and reduced their work force. 

select country, sum(total_laid_off)
from  world_layoffs.layoffs_staging2
group by country
order by 2 desc;
-- USA, India are most affected countries by these layoffs. 

select year(`date`), sum(total_laid_off)
from  world_layoffs.layoffs_staging2
group by year(`date`)
order by 1 desc;
-- 2022 is the world time for the layoffs
-- data is for time period 03/2020 to 03/2023
-- 2023 in only 3 months 125677 people got laid off 

select min(`date`), max(`date`)
from  world_layoffs.layoffs_staging2;
-- data is for time period 03/2020 to 03/2023

select substring(`date`,1,7) as `MONTH`, sum(total_laid_off)
from world_layoffs.layoffs_staging2
where substring(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT substring(`date`,1,7) as `MONTH`, sum(total_laid_off) AS TOTAL_OFF
FROM world_layoffs.layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`,  TOTAL_OFF , SUM(total_off) OVER (ORDER BY `MONTH`) AS ROLLING_TOTAL
FROM Rolling_Total;

SELECT COMPANY, YEAR(`DATE`), SUM(TOTAL_LAID_OFF)
FROM world_layoffs.layoffs_staging2
GROUP BY COMPANY, YEAR(`DATE`)
ORDER BY COMPANY ASC;
-- how much laidoffs are happened by company each year

WITH COMPANY_YEAR (COMPANY, YEARS, TOTAL_LAID_OFF) AS
(
SELECT COMPANY, YEAR(`DATE`), SUM(TOTAL_LAID_OFF)
FROM world_layoffs.layoffs_staging2
GROUP BY COMPANY, YEAR(`DATE`)
)
SELECT *, DENSE_RANK() OVER (PARTITION BY YEARS ORDER BY TOTAL_LAID_OFF DESC) AS RANKING
FROM COMPANY_YEAR
WHERE YEARS IS NOT NULL
ORDER BY RANKING ASC;
-- DISPLAY THE RANKING OF COMPANY FOR LAYOFF EACH YEAR

WITH COMPANY_YEAR (COMPANY, YEARS, TOTAL_LAID_OFF) AS
(
SELECT COMPANY, YEAR(`DATE`), SUM(TOTAL_LAID_OFF)
FROM world_layoffs.layoffs_staging2
GROUP BY COMPANY, YEAR(`DATE`)
), COMPANY_YEAR_RANK AS
(SELECT *, DENSE_RANK() OVER (PARTITION BY YEARS ORDER BY TOTAL_LAID_OFF DESC) AS RANKING
FROM COMPANY_YEAR
WHERE YEARS IS NOT NULL
)
SELECT * FROM COMPANY_YEAR_RANK
WHERE RANKING <=5 ;
-- YEAR WISE TOP 5 COMPANY FOR MAXIMUM LAYOFFS
-- THERE ARE 2 CTEs DEVELOPED WHERE 2ND CTE DEVELOPED FROM 1ST CTE 






















