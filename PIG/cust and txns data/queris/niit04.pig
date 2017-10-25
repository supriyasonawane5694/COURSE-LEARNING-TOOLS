

txn = load 'txns1' using PigStorage(',') as (txnid, txndate, custno:chararray, amount:double, cat, prod, city, state, type);

--dump txn;

txn = foreach txn generate $2, $3;
--dump txn;

grouptxn = group txn by custno;
--dump grouptxn;

totalspend = foreach grouptxn generate group, ROUND_TO(SUM(txn.amount),2) as total;
--dump totalspend;

--orderbytotal = order totalspend by $1 desc;

--dump orderbytotal;

top10 = limit (order totalspend by $1 desc) 10;
--dump top10;

cust = Load '/home/hduser/custs' using PigStorage(',') as (custno:chararray, firstname, lastname, age:int, profession:chararray);

--dump cust;

joined = join cust by $0, top10 by $0;
--dump joined;

--top10join = foreach joined generate $0, $1, $2, $3, $4, $6;
--dump top10join;

final = foreach joined generate ($0, $1, $2, $3, $4, $6) by $5 desc;
dump final;


--store final into 'pig/part04' using PigStore(',');

