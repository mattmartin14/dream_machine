INSTALL quack; LOAD quack;
CREATE SECRET quack_secret (TYPE quack, TOKEN 'get-to-da-choppa');
ATTACH 'quack:quackattack.yolomatt.com:9494' AS remote;

-- V 1.5.5 (works)
FROM remote.query('create or replace table dl1.sales (id int)');
FROM remote.query('insert into dl1.sales select * from range(0,20)');
FROM remote.query('select * from dl1.sales');

-- This Fall with v2.0
CONNECT remote;
create or replace table dl1.sales (id int);
insert into dl1.sales select * from range(0,20);
select * from dl1.sales;   
DISCONNECT;

