--Q.FIND THE COUNT OF TOTAL MODULE

step1 = Load '/home/hduser/serverdata' using PigStorage(' ') as (date,time,module:chararray,error_log:chararray,description:chararray);
--dump step1;

step2 = group step1 all;
--dump step2;

step4 = foreach step2 generate COUNT(step1.module);
dump step4;

