--Q.Count the number of logs of each type like trace log 7, error logs 12 and so on.


step1 = Load '/home/hduser/serverdata' using PigStorage(' ') as (date,time,module:chararray,error_log:chararray,description:chararray);
--dump step1;

step2 = foreach step1 generate error_log;
--dump step2;


step3 = group step2 by $0;
--dump step3;

step4 = foreach step3 generate COUNT(step2.$0);
dump step4;



