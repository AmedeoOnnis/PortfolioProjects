-- Title: Worldwide layoffs between 2020 and 2022
-- Process: Data Cleaning
-- Languages: SQL
-- Tools: MySQL
-- This dataset is available on Kaggle.com and contains data about the layoffs in the private sector between 2020 and the first part of 2023.

-- 1. Database selection and data preparation

USE world_layoffs;

SHOW TABLES;

-- 2. Construction of a new backup table

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- 3. Duplicates
-- Issue: there are no ID rows
-- Solution: use a CTE to create a new ID column, assign row number 1 to rows with unique values and delete those where row number is higher than 1.
-- Actually, these are the rows containing duplicates.

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

with duplicate_cte as
(select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

-- CTEs are useful to create a temporary table. However, they can't be used to directly update the original table as they are just temporary.
-- Solution: create a new backup table.

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

select * from layoffs_staging2 where row_num = 2;

-- There are five duplicates in this database.
-- The 'partition by' helps assigning a row number to those rows with exact duplicates in every column.
-- Then, five duplicates can be deleted without losing any data.

-- 4. Blank spaces around values.
-- Delete all the unnecessary blank spaces by using trim.

SELECT DISTINCT trim(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = trim(company);

SELECT * FROM layoffs_staging2;

-- 1.3 Same industry, different name.
-- Despite indicating the same sector, some industries may be referred to with different names.
-- This is the case for Crypto and Crypto-Currency.
-- Solution: standardize the value and update it for every row.

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- 5. Spelling mistakes in country column.
-- Datasets providing an international overview may have spelling mistakes in country and location.
-- While this is not the case for location, there's an issue in the country column. 

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States.';

-- "United States." and "United States" would be considered as two different countries during the analysis, but they'e actually the same country.
-- Solution: use trailing '.' command with a trim function.

SELECT DISTINCT country, trim(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = trim(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT DISTINCT country FROM layoffs_staging2 ORDER BY country DESC;

-- 6. Update the format for each column
-- By selecting a table via the navigator on the left, format and category of each column can be checked.
-- It's always better to convert columns with dates in a specific 'date' format and category and those with numbers in int or float.
-- Solution: specify the exact format via the str_to_date function and alter the table modifying the value category in each column.

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

ALTER TABLE layoffs_staging2
MODIFY COLUMN percentage_laid_off FLOAT;

SELECT * FROM layoffs_staging2;

-- 7. Useless rows
-- Useless data don't exist. However, when it comes to data analysis, it's crucial to make wise choices about which data should be analysed.
-- For example, let's consider an analysis of the number of layoffs per industry in a specific country.
-- For this kind of project, the rows where both 'total_laid_off' and 'percentage_laid_off' are null or empty can be left aside.

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Let's also take a look at 'industry'.

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

-- There are four rows where industry is not known: these rows can't be used for further analysis.
-- However, there are still the duplicates (row_num = 2) in our table.
-- Solution: have a look at the duplicates to see if the industry appears there and join it to the original row.

SELECT *
FROM layoffs_staging2
WHERE company = "Bally's Interactive";

-- The industry appears for three out of four rows. Let's delete the row of Bally's Interactive.
-- For the three rows remaining, let's join the industry value.

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Lastly, remove the duplicates and the row_num column.

DELETE
FROM layoffs_staging2
WHERE row_num = 2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Let's rename the table to complete the data cleaning!

ALTER TABLE layoffs_staging2
RENAME TO layoffs_clean;

SELECT * FROM layoffs_clean;

-- Thank you for checking my project! Feel free to interact and stay tuned for more projects!