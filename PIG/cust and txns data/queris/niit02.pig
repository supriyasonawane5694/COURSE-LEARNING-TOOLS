
--que to find the total count of customer for each profession, order - from highest to lowest



cust = Load '/home/hduser/custs' using PigStorage(',') as (custno:chararray, firstname, lastname, age:int, profession:chararray);

--dump cust;

groupbyprofession = group cust by profession;

--dump groupbyprofession;

countbyprofession = foreach groupbyprofession generate group as profession, COUNT(cust) as headcount;

--dump countbyprofession;

orderbycount = order countbyprofession by headcount desc;

--dump orderbycount;

store orderbycount into 'pig/part2' using PigStorage(',');

