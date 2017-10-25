--Q.Find the number of movies with duration more than 1.5 hours


movie = load '/home/hduser/MoviesDetails' using PigStorage(',') as (movieid:int, moviename:chararray, release_year:int, rating:double, duration );
--dump movie;

step1 = foreach movie generate movieid, duration;
--dump step1;

step2 = filter step1 by duration>5400;
--dump step2;


step3 = group step2 all;
--dump step3;


step4 = foreach step3 generate COUNT(step2.$0);
dump step4;


