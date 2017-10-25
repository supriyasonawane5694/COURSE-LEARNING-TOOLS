--Q.Find the module name which is generating maximum error logs



step1 = Load '/home/hduser/serverdata' using PigStorage(' ') as (date,time,module:chararray,error_log:chararray,description:chararray);
--dump step1;

step2 = foreach step1 generate module, error_log;
--dump step2;


step3 = filter step2 by error_log =='[ERROR]';
--dump step3;

x = group step3 by $0;
--dump x;

step4 = foreach x generate COUNT(step3.$0);
--dump step4;

step5 = group step4 all;
--dump step5;

y = foreach step5 generate MAX(step4.$0);
dump y;



