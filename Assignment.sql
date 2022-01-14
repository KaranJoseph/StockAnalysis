/* First step after creating assignment schema and importing stocks data, is to set the schema as default(for convenience).
Then make sure all the data has been properly imported by checking a count * of the imported tables.
*/
select count(*) from `bajaj auto`;
select count(*) from `eicher motors`;
select count(*) from `hero motocorp`;
select count(*) from `infosys`;
select count(*) from `tcs`;
select count(*) from `tvs motors`;

DROP Table IF EXISTS `bajaj1`; -- Drop the tables and functions that will be created again when running the script
DROP Table IF EXISTS `bajaj2`;
DROP Table IF EXISTS `eicher1`;
DROP Table IF EXISTS `eicher2`;
DROP Table IF EXISTS `hero1`;
DROP Table IF EXISTS `hero2`;
DROP Table IF EXISTS `infosys1`;
DROP Table IF EXISTS `infosys2`;
DROP Table IF EXISTS `tcs1`;
DROP Table IF EXISTS `tcs2`;
DROP Table IF EXISTS `tvs1`;
DROP Table IF EXISTS `tvs2`;
DROP Table IF EXISTS `Master Table`;
DROP Function IF EXISTS `Signal Function`;


/* #1. Create seperate tables which hold the 20 day MA and 50 day MA for respective stocks.

Note: Date is converted to required format, then sorted in ascending order.*/

CREATE TABLE bajaj1 AS
	SELECT  str_to_date(date,"%d-%M-%Y") as `Date`, `Close Price`,
		AVG(`Close Price`) OVER (rows 19 preceding) AS `20 Day MA` ,
		AVG(`Close Price`) OVER (rows 49 preceding) AS `50 Day MA`
	FROM  `bajaj auto`
order by `Date`;

CREATE TABLE eicher1 AS
	SELECT  str_to_date(date,"%d-%M-%Y") as `Date`, `Close Price`, 
		AVG(`Close Price`) OVER (rows 19 preceding) AS `20 Day MA` ,
		AVG(`Close Price`) OVER (rows 49 preceding) AS `50 Day MA`
	FROM  `eicher motors`
order by `Date`;

CREATE TABLE hero1 AS
	SELECT  str_to_date(date,"%d-%M-%Y") as `Date`, `Close Price`, 
		AVG(`Close Price`) OVER (rows 19 preceding) AS `20 Day MA` ,
		AVG(`Close Price`) OVER (rows 49 preceding) AS `50 Day MA`
	FROM  `hero motocorp`
order by `Date`;

CREATE TABLE infosys1 AS
	SELECT  str_to_date(date,"%d-%M-%Y") as `Date`, `Close Price`, 
		AVG(`Close Price`) OVER (rows 19 preceding) AS `20 Day MA` ,
		AVG(`Close Price`) OVER (rows 49 preceding) AS `50 Day MA`
	FROM  `infosys`
order by `Date`;

CREATE TABLE tcs1 AS
	SELECT  str_to_date(date,"%d-%M-%Y") as `Date`, `Close Price`, 
		AVG(`Close Price`) OVER (rows 19 preceding) AS `20 Day MA` ,
		AVG(`Close Price`) OVER (rows 49 preceding) AS `50 Day MA`
	FROM  `tcs`
order by `Date`;

CREATE TABLE tvs1 AS
	SELECT  str_to_date(date,"%d-%M-%Y") as `Date`, `Close Price`,
		AVG(`Close Price`) OVER (rows 19 preceding) AS `20 Day MA` ,
		AVG(`Close Price`) OVER (rows 49 preceding) AS `50 Day MA`
	FROM  `tvs motors`
order by `Date`;


/* #2. Create a Master Table having the dates and closing prices of all 6 stocks  */

Create Table `Master Table` as
	select A.`Date`,A.`Close Price` as `Bajaj`,
		E.`Close Price` as `TCS`,
		F.`Close Price` as `TVS`,
		D.`Close Price` as `Infosys`,
		B.`Close Price` as `Eicher`,
		C.`Close Price` as `Hero`

	from bajaj1 A

	join eicher1 B
	on A.`Date`=B.`Date`
	join hero1 C
	on A.`Date`=C.`Date`
	join infosys1 D
	on A.`Date`=D.`Date`
	join tcs1 E 
	on A.`Date`=E.`Date`
	join tvs1 F 
	on A.`Date`=F.`Date`;

/* #3. Using tables from #1, generate buy and sell signals, Store them in seperate tables wrt to each stock.

Note: Use cte to create a temp table with Difference, LaggedDifference and RowNumber columns.
	  RowNumber is used to remove the first 50 data rows from analysis as 50 day MA only calculates values from the 50th day onwards. 
	  Difference and LaggedDifference between 20 day MA and 50 day MA are used to calculate Signal :-
		  1. When Difference is positive and Lagged Difference is negative : Signal = Buy
		  2. When Difference is negative and Lagged Difference is positive : Signal = Sell
		  3. Else														   : Signal = Hold
        */


Create Table `bajaj2` as 
with cte as 
	(select `date`,`Close Price`,row_number()over() as RowNumber,
			`20 Day MA`-`50 Day MA` as Difference,
			lag(`20 Day MA`-`50 Day MA`) over() as LaggedDifference
	from `bajaj1`)
select `Date`,`Close Price`,
case
	when Difference>0 and LaggedDifference<0 then 'Buy'
    when Difference<0 and LaggedDifference>0 then 'Sell'
    else 'Hold'
end as `Signal`
from cte where RowNumber>50;

Create Table `eicher2` as 
with cte as 
	(select `date`,`Close Price`,row_number()over() as RowNumber,
			`20 Day MA`-`50 Day MA` as Difference,
			lag(`20 Day MA`-`50 Day MA`) over() as LaggedDifference
	from `eicher1`)
select `Date`,`Close Price`,
case
	when Difference>0 and LaggedDifference<0 then 'Buy'
    when Difference<0 and LaggedDifference>0 then 'Sell'
    else 'Hold'
end as `Signal`
from cte where RowNumber>50;

Create Table `hero2` as 
with cte as 
	(select `date`,`Close Price`,row_number()over() as RowNumber,
			`20 Day MA`-`50 Day MA` as Difference,
			lag(`20 Day MA`-`50 Day MA`) over() as LaggedDifference
	from `hero1`)
select `Date`,`Close Price`,
case
	when Difference>0 and LaggedDifference<0 then 'Buy'
    when Difference<0 and LaggedDifference>0 then 'Sell'
    else 'Hold'
end as `Signal`
from cte where RowNumber>50;

Create Table `infosys2` as 
with cte as 
	(select `date`,`Close Price`,row_number()over() as RowNumber,
			`20 Day MA`-`50 Day MA` as Difference,
			lag(`20 Day MA`-`50 Day MA`) over() as LaggedDifference
	from `infosys1`)
select `Date`,`Close Price`,
case
	when Difference>0 and LaggedDifference<0 then 'Buy'
    when Difference<0 and LaggedDifference>0 then 'Sell'
    else 'Hold'
end as `Signal`
from cte where RowNumber>50;

Create Table `tcs2` as 
with cte as 
	(select `date`,`Close Price`,row_number()over() as RowNumber,
			`20 Day MA`-`50 Day MA` as Difference,
			lag(`20 Day MA`-`50 Day MA`) over() as LaggedDifference
	from `tcs1`)
select `Date`,`Close Price`,
case
	when Difference>0 and LaggedDifference<0 then 'Buy'
    when Difference<0 and LaggedDifference>0 then 'Sell'
    else 'Hold'
end as `Signal`
from cte where RowNumber>50;

Create Table `tvs2` as 
with cte as 
	(select `date`,`Close Price`,row_number()over() as RowNumber,
			`20 Day MA`-`50 Day MA` as Difference,
			lag(`20 Day MA`-`50 Day MA`) over() as LaggedDifference
	from `tvs1`)
select `Date`,`Close Price`,
case
	when Difference>0 and LaggedDifference<0 then 'Buy'
    when Difference<0 and LaggedDifference>0 then 'Sell'
    else 'Hold'
end as `Signal`
from cte where RowNumber>50;

/* 4. Create a Function for Bajaj Stock, that takes Date Input and Returns its Signal for that day. */

delimiter $$
create function `Signal Function` ( InputDate date )
returns nvarchar(255)
deterministic
begin
declare val nvarchar(255);
select `Signal` into val from bajaj2 where date=InputDate;
set val=if(val is null , 'Invalid date - Please Enter Different Date',val);
return val;
end$$
delimiter ;

-- DROP function IF EXISTS `Signal Function`;


select `Signal Function`('2015-03-18') as `Signal`; -- Hold
select `Signal Function`('2018-05-29') as `Signal`; -- Sell
select `Signal Function`('2015-10-19') as `Signal`; -- Buy
select `Signal Function`('2018-07-30') as `Signal`; -- Hold
select `Signal Function`('2015-12-23') as `Signal`; -- Sell

