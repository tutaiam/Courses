--#Database stucture --

create table if not exists shops
(
	shop_id integer PRIMARY KEY,
	shop_name varchar(100)
);


create table if not exists products
(
	product_id integer PRIMARY KEY,
	product_name varchar(100),
	price numeric(7, 2)
);

create table if not exists plan 
(
	product_id integer references products (product_id) on delete cascade on update cascade,
	shop_id integer references shops (shop_id) on delete cascade on update cascade,
	plan_cnt integer,
	plan_date date
);

create table if not exists shop_mvideo 
(
	date date,
	shop_id integer references shops (shop_id) on delete cascade on update cascade,
	product_id integer references products (product_id) on delete cascade on update cascade,
	sales_cnt integer
);

create table if not exists shop_dns
(
	date date,
	shop_id integer references shops (shop_id) on delete cascade on update cascade,
	product_id integer references products (product_id) on delete cascade on update cascade,
	sales_cnt integer
);

create table if not exists shop_sitilink 
(
	date date,
	shop_id integer references shops (shop_id) on delete cascade on update cascade,
	product_id integer references products (product_id) on delete cascade on update cascade,
	sales_cnt integer
);

-- #Data load --

COPY shops
FROM '/docker-entrypoint-initdb.d/data/shops.csv'
DELIMITER ','
ENCODING 'UTF8'
CSV HEADER;


COPY products
FROM '/docker-entrypoint-initdb.d/data/products.csv'
DELIMITER ','
ENCODING 'UTF8'
CSV HEADER;

COPY shop_dns
FROM '/docker-entrypoint-initdb.d/data/shop_dns.csv'
DELIMITER ','
ENCODING 'UTF8'
CSV HEADER;

COPY shop_mvideo
FROM '/docker-entrypoint-initdb.d/data/shop_mvideo.csv'
DELIMITER ','
ENCODING 'UTF8'
CSV HEADER;

COPY shop_sitilink
FROM '/docker-entrypoint-initdb.d/data/shop_sitilink.csv'
DELIMITER ','
ENCODING 'UTF8'
CSV HEADER;

COPY plan
FROM '/docker-entrypoint-initdb.d/data/plan.csv'
DELIMITER ','
ENCODING 'UTF8'
CSV HEADER;