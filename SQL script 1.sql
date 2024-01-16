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
