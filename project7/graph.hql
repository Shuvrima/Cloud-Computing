drop table Graph;
create table Graph(
  id int,
  node int)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table Graph;

select node, count(node) as cnt
from Graph
group by node 
order by cnt desc;

