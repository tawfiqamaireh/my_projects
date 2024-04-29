SELECT * FROM layoffs;

DROP TABLE IF EXISTS layoffs_staging;
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;





with duplicate_cte as 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
select * 
from duplicate_cte
where row_num >1;


select *
from layoffs_staging
where company='Casper';




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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




INSERT INTO layoffs_staging2 
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;



SELECT * FROM layoffs_staging2;


##Standarizing Data

select  company,trim(company)
from layoffs_staging2;


DELETE FROM layoffs_staging2
WHERE row_num >1 ;


UPDATE layoffs_staging2
SET company=trim(company);


select distinct(industry)
from layoffs_staging2;


select *
from layoffs_staging2
where industry like 'Crypto%';


UPDATE layoffs_staging2
set industry ='Crypto'
where industry like 'Crypto%';



select distinct(location)
from layoffs_staging2;



select *
from layoffs_staging2
where country like 'United State%';


select  distinct country,trim(trailing '.' from country)
from layoffs_staging2
order by 1;


update layoffs_staging2
set country=trim(trailing '.' from country);



select  distinct country
from layoffs_staging2
order by 1;



select 	`date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;


update layoffs_staging2
set  `date`=str_to_date(`date`,'%m/%d/%Y');



select 	`date`
from layoffs_staging2;


Alter table layoffs_staging2
modify column `date` date;

select *  from layoffs_staging2
where total_laid_off is null;


select *  from layoffs_staging2
where industry is null 
or industry ='';


select *  from layoffs_staging2
where company like 'Bally''s Interactive%';

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;


update layoffs_staging2
set industry =null
where industry ='';


update layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null
and t2.industry is not null;



select *  from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null ;

delete from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null ;



select *  from layoffs_staging2;


alter table layoffs_staging2
drop column row_num;