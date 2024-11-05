select * from layoffs;
drop table layoffs_staging;
create table layoffs_staging
Like layoffs;

select * from layoffs_staging;


insert layoffs_staging
select * from layoffs;
With duplicate_cts as(
select *,
row_number() over( partition by company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
delete from duplicate_cts where row_num>1;
drop table layoffs_staging2;
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
select * from layoffs_staging2;
insert into layoffs_staging2
select *,
row_number() over(partition by 
company, location, industry,
 total_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;


select * from layoffs_staging2
where row_num>1;

Delete from layoffs_staging2
where row_num>1;

select company, Trim(company) from layoffs_staging2;
update layoffs_staging2
set company=trim(company);

select * from layoffs_staging2;
update layoffs_staging2
set industry='Crypto'
where industry like 'crypto%';

select * from layoffs_staging2
where industry like 'crypto%'
;

select distinct industry from layoffs_staging2
where industry like 'crypto%';

;
update layoffs_staging2
set country='Untited state'
where country like 'united state%';

select distinct country from layoffs_staging2 order by 1;

select `date` ,
STR_To_Date(`date`, '%m/%d/%Y')
from layoffs_staging2
;

update layoffs_staging2 
set `date`= STR_To_Date(`date`, '%m/%d/%Y');

select * from layoffs_staging2;
Alter table layoffs_staging2
modify `date` date;

select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null
;

delete
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null
;

select * from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

select max(total_laid_off) , max(percentage_laid_off) from layoffs_staging2;

select * from layoffs_staging2 
where percentage_laid_off=1
order by funds_raised_millions desc;

select * from layoffs_staging2; 

select company, sum(total_laid_off) 
from layoffs_staging2
group by company
order by 2 DESC
;

select industry, sum(total_laid_off) 
from layoffs_staging2
group by industry
order by 2 DESC
;

select year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by year(`date`)
order by 1 DESC
;


With rolling_total as(
select substring(`date`,1,7) as month, sum(total_laid_off) as sum_laidoff
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 ASC)
select `month`, sum_laidoff, sum(sum_laidoff)
over( order by `month`) as roll
from rolling_total
;
With company_year as(
select company, year(`date`) as dd, sum(total_laid_off) as tt
from layoffs_staging2
group by company, year(`date`)),
company_year_rank as(
select *, dense_rank() over( partition by dd order by tt Desc) as ranking 
from company_year
where dd is not null)
select * from company_year_rank
where ranking<=5
;