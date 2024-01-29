--Calculating no of athletes in each country
SELECT Country, COUNT(PersonName) as NoOFPerson
FROM athletes
GROUP BY Country
Order by Country asc;

--Calculate total medals won by each country
SELECT Team_Country,
sum(Gold) TotalGold,
sum(Silver) TotalSilver,
sum(Bronze) TotalBronze 
from medals
group by Team_Country;

-- calculate average number of
 entries by gender for each discipline
select Discipline,
avg(Female) AvgFemale, 
avg(Male) AvgMale
from entriesgender
where Discipline = 'Artistic Gymnastics'
group by Discipline

--Top 10 Countries
select distinct m.Team_Country, m.Rank_by_Total
from medals m
join teams t on m.Team_Country = t.Country
where Rank_by_Total <= 10
order by m.Rank_by_Total;

--grouping  athletes by discipline
select count(discipline), discipline
from athletes
group by discipline
limit 5;
