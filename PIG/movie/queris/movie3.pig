--Q.Count the total number of movies in the list


movie = load '/home/hduser/MoviesDetails' using PigStorage(',') as (movieid:int, moviename:chararray, release_year:int, rating:double, duration );
--dump movie;


step1 = group movie all;
--dump step1;

step3 =  FOREACH step1 GENERATE group, COUNT(movie);
dump step3;




