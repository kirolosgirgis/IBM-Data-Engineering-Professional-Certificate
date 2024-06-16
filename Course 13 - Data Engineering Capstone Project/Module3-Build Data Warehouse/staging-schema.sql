BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging."softcarDimDate"
(
    dateid integer NOT NULL,
    date date NOT NULL,
    day integer NOT NULL,
    weekday integer NOT NULL,
    weekdayname character varying(10),
    month integer NOT NULL,
    monthname character varying(10),
    quarter integer NOT NULL,
    quartername character varying(2),
    year integer NOT NULL,
    PRIMARY KEY (dateid)
);

CREATE TABLE IF NOT EXISTS staging."softcarDimCategory"
(
    categoryid integer NOT NULL,
    category character varying(20) NOT NULL,
    PRIMARY KEY (categoryid)
);
    
CREATE TABLE IF NOT EXISTS staging."softcarDimItem"
(
    itemid integer NOT NULL,
    item character varying(50) NOT NULL,
    PRIMARY KEY (itemid)
);

CREATE TABLE IF NOT EXISTS staging."softcartDimCountry"
(
    countryid integer NOT NULL,
    country character varying(50) NOT NULL,
    PRIMARY KEY (countryid)
);

CREATE TABLE IF NOT EXISTS staging."softcartFactSales"
(
    orderid integer NOT NULL,
    dateid integer NOT NULL,
    categoryid integer NOT NULL,
    itemid integer NOT NULL,
    countryid integer NOT NULL,
    amount integer NOT NULL,
    PRIMARY KEY (orderid)
);

ALTER TABLE staging."softcartFactSales"
    ADD FOREIGN KEY (dateid)
    REFERENCES staging."softcartDimDate" (dateid)
    NOT VALID;


ALTER TABLE staging."softcartFactSales"
    ADD FOREIGN KEY (categoryid)
    REFERENCES staging."softcartDimCategory" (categoryid)
    NOT VALID;


ALTER TABLE staging."softcartFactSales"
    ADD FOREIGN KEY (itemid)
    REFERENCES staging."softcartDimItem" (itemid)
    NOT VALID;


ALTER TABLE staging."softcartFactSales"
    ADD FOREIGN KEY (countryid)
    REFERENCES staging."softcartDimCountry" (countryid)
    NOT VALID;

END;