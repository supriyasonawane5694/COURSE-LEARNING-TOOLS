--Q.find the number of movies released between 1945 and 1959


movie = load '/home/hduser/MoviesDetails' using PigStorage(',') as (movieid:int, moviename:chararray, release_year:int, rating:double, duration );
--dump movie;

step1 = foreach movie generate movieid, release_year;
--dump step1;

step2 = filter step1 by release_year>1945 and release_year<1959;
--dump step2;


step3 = group step2 all;
--dump step3;


step4 = foreach step3 generate COUNT(step2.$0);
dump step4;


