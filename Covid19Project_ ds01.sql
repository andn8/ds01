/*
Covid 19 Data Exploration 

Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types

*/
--truy vấn dữ liệu với continent not null
select * 
  from Project_Exploration_covid19.dbo.CovidDeaths
 where continent is not null 
 order by 3, 4 ;
 --truy vấn location , date , new case , total_deaths , populations 
 select location,date,total_cases, new_cases, total_deaths , population
   from Project_Exploration_covid19.dbo.CovidDeaths
  where continent is not null 
  order by 1,6; 
  -- tỉ lệ ca tử vong trên tổng số ca mắc ở Mỹ 
select location , date , total_cases , total_deaths , (total_deaths/total_cases)*100 as DeathPercentage 
  from Project_Exploration_covid19.dbo.CovidDeaths
 where location like '%state%'
   and continent is not null 
 order by 1,2 ; 
 -- tỉ lệ ca nhiễm Covid_19 trên tổng dân số ở Mỹ 
select location , date , total_cases , round((total_cases/population)*100 , 6 )  as PercentPopulationInfected 
  from Project_Exploration_covid19.dbo.CovidDeaths
 where location like '%state%'
   and continent is not null 
 order by 2,4
 --- truy vấn số ca mắc nhiều nhất của từng nước , và tỉ lệ số ca mắc trên tổng số dân là lớn nhất 
 select location,population , MAX(total_cases) as HighestInfectionCount , 
        round ( MAX((total_cases/population)*100) , 2 )  as PercentPopulationInfected
   from Project_Exploration_covid19.dbo.CovidDeaths
  --where location like '%state%'
  --  and continent is not null 
  group by location , population 
  --having MAX(total_cases) > 1000000
  order by 1 , 2 
-- truy vấn số ca chết do covid_19 lớn nhất của từng nước khu vực Asia 
select location , MAX(cast(total_deaths as int)) as TotalDeathsCount 
  from Project_Exploration_covid19.dbo.CovidDeaths
 where continent = 'Asia'
 group by location
 --having MAX(cast(total_deaths as int)) > 10000
 order by TotalDeathsCount desc 



 -- truy vấn số ca chết đạt max theo khu vực 
 select continent,MAX(cast(total_deaths as int))  as CotinentTotalDeathsCount
   from Project_Exploration_covid19.dbo.CovidDeaths
  where continent is not null
  group by continent
  order by CotinentTotalDeathsCount desc

-- tổng số ca mắc mới , tổng số ca chết mới , tỉ lệ số ca chết / số ca mắc theo khu vực 
select continent , SUM(new_cases) as total_new_cases , SUM(cast(new_deaths as int)) as total_new_deaths ,
     round(( SUM(cast(new_deaths as int)) / SUM(new_cases) ) , 4 ) * 100 as DeathsPercentage 
  from Project_Exploration_covid19.dbo.CovidDeaths
 where continent is not null 
 group by continent 
 order by 2 ,3 
----tìm hiểu về windown function 
-- tính tổng số vaccine được tiêm đến tại thời điểm tính , theo từng location 
select d.continent , d.location , d.date , d.population , v.new_vaccinations , 
       SUM(convert(int,v.new_vaccinations)) over ( partition by d.location order by d.location , d.date) as RollingPeopleVaccinated
  from Project_Exploration_covid19.dbo.CovidDeaths d
  join Project_Exploration_covid19.dbo.CovidVaccinations v
    on v.location=d.location
   and v.date=d.date 
 where d.continent is not null 
 order by 2 ,3 

-- tính tổng số vaccine được tiêm đến tại thời điểm tính ,với location = 'United States'
select d.continent , d.location ,d.date , d.population , v.new_vaccinations, 
       SUM(convert(int ,v.new_vaccinations)) over ( partition by d.location order by d.location , d.date) as RollingPeopleVaccinated 
  from Project_Exploration_covid19.dbo.CovidDeaths d
  join Project_Exploration_covid19.dbo.CovidVaccinations v 
    on v.location = d.location
   and v.date = d.date
 where d.continent is not null and d.location= 'United States' 
 order by d.date desc , RollingPeopleVaccinated desc
 -- Using CTE to perform Calculation on Partition By in previous query
 tính tỉ lệ người dân được tiêm vaccine trên tổng dân số tới thời điểm tính , theo từng location 
 with PopvsVac (continent , Location , date , Population ,New_vaccinations , RollingPeopleVaccinated ) 
   as ( 
		select d.continent , d.location , d.date , d.population , v.new_vaccinations , 
			   SUM(convert(int,v.new_vaccinations)) over ( partition by d.location order by d.location , d.date) as RollingPeopleVaccinated
		  from Project_Exploration_covid19.dbo.CovidDeaths d
		  join Project_Exploration_covid19.dbo.CovidVaccinations v
			on v.location=d.location
		   and v.date=d.date 
		 where d.continent is not null 
		 --order by 2 ,3      
		 )
select * ,( RollingPeopleVaccinated/Population)*100 as People_Vacinated_percentage
  from PopvsVac
-- Using Temp Table to perform Calculation on Partition By in previous query
Drop table if exists #PercentPopulationVaccinated
Create table #PercentPopulationVaccinated 
( 
  continent nvarchar(255) , 
  location nvarchar(255) , 
  date datetime , 
  population numeric , 
  New_vaccinations numeric , 
  RollingPeopleVaccinated numeric 
  ) 
insert into #PercentPopulationVaccinated
select d.continent , d.location , d.date , d.population , v.new_vaccinations , 
			   SUM(convert(int,v.new_vaccinations)) over ( partition by d.location order by d.location , d.date) as RollingPeopleVaccinated
  from Project_Exploration_covid19.dbo.CovidDeaths d
  join Project_Exploration_covid19.dbo.CovidVaccinations v
    on v.location=d.location
   and v.date=d.date 
 where d.continent is not null
select * , (RollingPeopleVaccinated/Population)*100 as ngocphuongwithlove 
  from #PercentPopulationVaccinated
-- Creating View to store data for later visualizations
create view ngocphuongxinhgai as 
select d.continent , d.location , d.date , d.population , v.new_vaccinations , 
			   SUM(convert(int,v.new_vaccinations)) over ( partition by d.location order by d.location , d.date) as RollingPeopleVaccinated
  from Project_Exploration_covid19.dbo.CovidDeaths d
  join Project_Exploration_covid19.dbo.CovidVaccinations v
    on v.location=d.location
   and v.date=d.date 
 where d.continent is not null
 -- truy vấn lại table view 
 select * from ngocphuongxinhgai