--Load the sales data

txn = load 'txns1' using PigStorage(',') as (txnid, txndate, custno:chararray, amount:double, cat, prod, city, state, type);

txn = foreach txn generate custno, amount;

--dump txn;
--mapper

grouptxn = group txn by custno;
--dump grouptxn;
--describe grouptxn;
--grouptxn: {group: chararray,txn: {(custno: chararray,amount: double)}}

totalspend = foreach grouptxn generate group, ROUND_TO(SUM(txn.amount),2) as total;

--dump totalspend;

--orderbytotal = order totalspend by $1 desc;

top10 = limit (order totalspend by $1 desc) 10;

--dump top10;

cust = Load '/home/hduser/custs' using PigStorage(',') as (custno:chararray, firstname, lastname, age:int, profession:chararray);

joined = join cust by $0, top10 by $0;

--dump joined;

--top10join = foreach joined generate $0, $1, $2, $3, $4, $6;

--dump top10join;

final = order (foreach joined generate $0, $1, $2, $3, $4, $6) by $5 desc;

--dump final;

store final into 'pig/niit20' using PigStorage(',');




























