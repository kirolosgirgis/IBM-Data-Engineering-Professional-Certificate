BEGIN;

CREATE TABLE public."DimStore"
(
	storeid INTEGER NOT NULL,
	city VARCHAR(50) NOT NULL,
	country VARCHAR(50) NOT NULL,
    PRIMARY KEY (storeid)
);
	
CREATE TABLE public."DimDate"
(
	dateid INTEGER NOT NULL,
	day INTEGER NOT NULL,
	weekday INTEGER NOT NULL,
	weekdayname VARCHAR(10) NOT NULL,
	year INTEGER NOT NULL,
	month INTEGER NOT NULL,
	monthname VARCHAR(10) NOT NULL,
	quarter INTEGER NOT NULL,
	quertername VARCHAR(2),
    PRIMARY KEY (dateid)
);

CREATE TABLE public."FactSales"
(
	rowid INTEGER NOT NULL,
	storeid INTEGER NOT NULL,
	dateid INTEGER NOT NULL,
	totalsales INTEGER NOT NULL,
    PRIMARY KEY (rowid)
);

ALTER TABLE public."FactSales"
	ADD FOREIGN KEY (storeid)
    REFERENCES public."DimStore" (storeid)
    NOT VALID;


ALTER TABLE public."FactSales"
    ADD FOREIGN KEY (dateid)
    REFERENCES public."DimDate" (dateid)
    NOT VALID;


END;