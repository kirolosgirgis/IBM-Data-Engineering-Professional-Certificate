BEGIN;

CREATE TABLE IF NOT EXISTS public."DimDate"
(
    dateid INTEGER NOT NULL,
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    quartername VARCHAR(2),
    month INTEGER NOT NULL,
    monthname VARCHAR(10),
    day INTEGER NOT NULL,
    weekday INTEGER NOT NULL,
    weekdayname VARCHAR(10),
    PRIMARY KEY (dateid)
);

CREATE TABLE IF NOT EXISTS public."DimCategory"
(
    categoryid INTEGER NOT NULL,
    category VARCHAR(20) NOT NULL,
    PRIMARY KEY (categoryid)
);

CREATE TABLE IF NOT EXISTS public."DimCountry"
(
    countryid INTEGER NOT NULL,
    country VARCHAR(50) NOT NULL,
    PRIMARY KEY (countryid)
);

CREATE TABLE IF NOT EXISTS public."FactSales"
(
    orderid INTEGER NOT NULL,
    dateid INTEGER NOT NULL,
    countryid INTEGER NOT NULL,
    categoryid INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    PRIMARY KEY (orderid)
);

ALTER TABLE public."FactSales"
    ADD FOREIGN KEY (dateid)
    REFERENCES public."DimDate" (dateid)
    NOT VALID;


ALTER TABLE public."FactSales"
    ADD FOREIGN KEY (countryid)
    REFERENCES public."DimCountry" (countryid)
    NOT VALID;


ALTER TABLE public."FactSales"
    ADD FOREIGN KEY (categoryid)
    REFERENCES public."DimCategory" (categoryid)
    NOT VALID;


END;