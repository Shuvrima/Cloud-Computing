A= LOAD '$G' USING PigStorage(',') AS (x:int, y:int);
B= group A by y;
C= foreach B generate group,COUNT(A.y) as new;
D = order C by new desc;
STORE D INTO '$O' USING PigStorage (',');
