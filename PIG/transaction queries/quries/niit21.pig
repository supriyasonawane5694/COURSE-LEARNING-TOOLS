txn = load 'txns1' using PigStorage(',') as (txnid, txndate, custno:chararray, amount:double, cat, prod, city, state, type);

txn = foreach txn generate type, amount;

grouptxn = group txn by type;

totalspend = foreach grouptxn generate group, ROUND_TO(SUM(txn.amount),2) as total;

--dump totalspend;


totalgroup = group totalspend all;

--describe totalgroup;

--totalgroup: {group: chararray,totalspend: {(group: bytearray,total: double)}}

--dump totalgroup;

totalsales = foreach totalgroup generate ROUND_TO(SUM(totalspend.$1),2);

--dump totalsales;

final = foreach totalspend generate $0, $1, ROUND_TO(($1/totalsales.$0)*100,2);

--dump final;

store final into 'pig/niit21' using PigStorage(',');




