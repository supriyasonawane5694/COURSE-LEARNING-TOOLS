--Q.Find the number of movies having rating more than 3.9

movie = load '/home/hduser/MoviesDetails' using PigStorage(',') as (movieid:int, moviename:chararray, release_year:int, rating:double, duration );
--dump movie;

step1 = foreach movie generate movieid, rating;
--dump step1;

step2 = FILTER step1 by rating>3.9;
--dump step2;

step3 = group step2 all;
--dump step3;

step4 = foreach step3 generate COUNT(step2.$0);
dump step4;

