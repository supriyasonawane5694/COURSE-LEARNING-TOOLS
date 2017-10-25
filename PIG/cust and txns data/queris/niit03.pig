txn = load '/home/hduser/txns1' using PigStorage(',') as (txnid, txndate, custno:chararray, amount:double, cat, prod, city, state, type);
--dump txn;
txn = foreach txn generate type, amount;
--dump txn;
grouptxn = group txn by type;

--dump grouptxn;

totalspend = foreach grouptxn generate group , ROUND_TO(SUM(txn.amount),2) as total;
--dump totalspend;

totalgroup = group totalspend all;
--dump totalgroup;

totalsales = foreach totalgroup generate ROUND_TO(SUM(totalspend.$1),2);
--dump totalsales;


final = foreach totalspend generate $0, $1, ROUND_TO(($1/totalsales.$0)*100,2);

dump final;


