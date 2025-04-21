-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- Goal of the project
-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways


-- world_layoffs.layoffs - data table
-- world_layoffs.layoffs_staging - staging data table with copy of data table
-- world_layoffs.layoffs_staging2 - table for data analysis with including row_num to find duplicates


SELECT * 
FROM world_layoffs.layoffs_staging;

SELECT count(*) 
FROM world_layoffs.layoffs;

CREATE TABLE world_layoffs.layoffs_staging
LIKE world_layoffs.layoffs;

INSERT world_layoffs.layoffs_staging
SELECT * 
FROM world_layoffs.layoffs;

-- 1. Remove Duplicates

# First let's check for duplicates

-- window function for getting the duplicates
SELECT *,
ROW_NUMBER()  OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM world_layoffs.layoffs_staging;

-- CTE using the window function to fetch the duplicate rows
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER()  OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM world_layoffs.layoffs_staging
)
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1; -- 7 rows

SELECT * 
FROM world_layoffs.layoffs_staging
WHERE company = 'Casper';

SELECT * 
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda';

-- After analysis realised we need to improve the filter condition in partition including every columns

DESC world_layoffs.layoffs_staging;

SELECT *,
ROW_NUMBER()  OVER(
PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) as row_num
FROM world_layoffs.layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER()  OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) as row_num
FROM world_layoffs.layoffs_staging
)


SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;  -- 5 rows

SELECT * 
FROM world_layoffs.layoffs_staging
WHERE company = 'Cazoo';

-- After analysis realised we need to handle NULLs

CREATE TABLE world_layoffs.layoffs_staging2 (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM world_layoffs.layoffs_staging2;

INSERT INTO world_layoffs.layoffs_staging2
SELECT *,
ROW_NUMBER()  OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) as row_num
FROM world_layoffs.layoffs_staging;

SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

DELETE
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- 2. Standardize Data

SELECT company, trim(company) 
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET company = trim(company) ;

SELECT DISTINCT industry 
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- 'Crypto', 'Crypto Currency', 'CryptoCurrency' needs to be changed as 'Crypto'


SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE industry like '%Crypto%';

UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE '%Crypto%';

SELECT *
FROM world_layoffs.layoffs_staging2;

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE country LIKE '%United States.%'; -- 4 rows

UPDATE world_layoffs.layoffs_staging2
SET country =  'United States'
WHERE country LIKE '%United States.%';

-- change date column data type from text to date

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');  -- initially didn't work as there is text 'NULL' present in date field

-- As every field is text so Nulls are also text with value "NULL"

SELECT DISTINCT `date` 
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

SELECT * FROM  world_layoffs.layoffs_staging2
WHERE `date` = 'NULL';

UPDATE  world_layoffs.layoffs_staging2
SET `date` = NULL
WHERE `date` = 'NULL' ; -- we converted text 'NULL' to NULL

SELECT * FROM  world_layoffs.layoffs_staging2
WHERE `date` IS NULL;

UPDATE world_layoffs.layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y'); -- now it worked after converting text 'Null' to Null 

UPDATE world_layoffs.layoffs_staging2
SET `date` = CASE
    WHEN `date` LIKE '%/%/%' THEN STR_TO_DATE(`date`, '%m/%d/%Y')
    ELSE `date`
END; -- i didn't use this query but looks usefull.

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;


-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values 


SELECT DISTINCT industry
FROM  world_layoffs.layoffs_staging2
ORDER BY 1; -- it has a space and 'NULL' value ;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry = 'NULL' 
OR industry is NULL
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

-- nothing wrong here
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

-- we should set the blanks to nulls since those are typically easier to work with

SELECT * 
FROM  world_layoffs.layoffs_staging2
WHERE company = 'airbnb';

UPDATE  world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE  world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = 'NULL';

SELECT * 
FROM  world_layoffs.layoffs_staging2
WHERE industry is NULL;

-- now we need to populate those nulls if possible


SELECT t1.industry, t2.industry 
FROM  world_layoffs.layoffs_staging2  t1
JOIN world_layoffs.layoffs_staging2  t2
	ON t1.company = t2.company
-- set t1.industry = t2.industry
WHERE t1.industry is NULL
AND t2.industry is NOT NULL; 


UPDATE world_layoffs.layoffs_staging2  t1
JOIN world_layoffs.layoffs_staging2  t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;  

-- and if we check it looks like Bally's was the only one without a populated row to populate this null values

-- 4. remove any columns and rows we need to

SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off = 'NULL'
AND percentage_laid_off = 'NULL';

DELETE
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off = 'NULL'
AND percentage_laid_off = 'NULL';

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;


    
    












