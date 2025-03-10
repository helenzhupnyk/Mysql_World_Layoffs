-- Selecting all records from the layoffs table
SELECT * 
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Handle Null or Blank Values
-- 4. Remove Unnecessary Columns

-- Creating a new staging table with the same structure as layoffs
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Verifying the newly created staging table
SELECT * 
FROM layoffs_staging;

-- Copying all data from layoffs to layoffs_staging
INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- 1. Identifying Duplicate Records Using ROW_NUMBER()
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Using CTE to find duplicates across multiple columns
WITH duplicate_cte AS (
    SELECT *,
    ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

-- Checking records for a specific company to verify duplicates
SELECT * 
FROM layoffs_staging
WHERE company = "Casper";

-- Attempting to delete duplicates (Incorrect method, will not work)
WITH duplicate_cte AS (
    SELECT *,
    ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;

-- Creating a new staging table with an additional column for row numbers
CREATE TABLE layoffs_staging2 (
  company TEXT,
  location TEXT,
  industry TEXT,
  total_laid_off INT DEFAULT NULL,
  percentage_laid_off TEXT,
  `date` TEXT,
  stage TEXT,
  country TEXT,
  funds_raised_millions INT DEFAULT NULL,
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Checking the newly created layoffs_staging2 table
SELECT * 
FROM layoffs_staging2;

-- Inserting data into layoffs_staging2 while adding row numbers to identify duplicates
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Deleting duplicate records based on row_num
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

-- Verifying data after removing duplicates
SELECT * 
FROM layoffs_staging2;

-- 2. Standardizing the Data
-- Trimming spaces from company names
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Updating company names to remove leading/trailing spaces
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Checking distinct industry values
SELECT DISTINCT industry
FROM layoffs_staging2;

-- Identifying records with industry names starting with 'Crypto'
SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Standardizing industry names to 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Checking distinct country values
SELECT DISTINCT country
FROM layoffs_staging2;

-- Identifying inconsistencies in country names
SELECT *
FROM layoffs_staging2
WHERE country LIKE "United States%"
ORDER BY 1;

-- Standardizing country names by removing trailing dots
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE "United States%";

-- Converting date column to proper date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Modifying date column to DATE type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Handling Null or Blank Values
-- Identifying records where both total_laid_off and percentage_laid_off are NULL
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Checking for missing industry values
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

-- Identifying missing industry values for a specific company
SELECT *
FROM layoffs_staging2
WHERE company = "Airbnb";

-- Updating blank industry values to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Filling missing industry values using matching company records
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- Identifying records with incorrect company name pattern
SELECT *
FROM layoffs_staging2
WHERE company LIKE "Bally%";

-- Removing records where both total_laid_off and percentage_laid_off are NULL
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Final check after cleaning
SELECT *
FROM layoffs_staging2;

-- 4. Removing Unnecessary Columns
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
