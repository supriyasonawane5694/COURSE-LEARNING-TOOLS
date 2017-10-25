--Q.Find the list of years and number of movies released each year?

movie = load '/home/hduser/MoviesDetails' using PigStorage(',') as (movieid:int, moviename:chararray, release_year:int, rating:double, duration );
--dump movie;


--step1 = foreach movie generate movieid, release_year;
--dump step1;

step2 = group movie by release_year;
--dump step2;


step3 = foreach step2 generate group, COUNT(movie);
dump step3;

