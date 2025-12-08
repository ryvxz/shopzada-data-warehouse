# Data Ingestion and Quality Report

**Generated On:** 2025-12-08 10:40:15

**Total Tables Processed:** 8


---


## 📊 Table: `customer_user_credit_card`

**Final Row Count:** 5,000

**Column Count:** 4

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|                    | Data Type   |
|:-------------------|:------------|
| user_id            | object      |
| name               | object      |
| credit_card_number | int64       |
| issuing_bank       | object      |


### Sample Data (First 5 Rows)


| user_id   | name             |   credit_card_number | issuing_bank   |
|:----------|:-----------------|---------------------:|:---------------|
| user40678 | zion feest       |           4294956114 | bpi            |
| user08728 | kattie bergstrom |           2742902159 | bdo            |
| user29759 | aiden corwin     |           1917471950 | bdo            |
| user16806 | vince gislason   |           3290792253 | chinabank      |
| user27644 | adele okuneva    |           2313395832 | chinabank      |


---


## 📊 Table: `customer_user_data`

**Final Row Count:** 5,000

**Column Count:** 11

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|                | Data Type   |
|:---------------|:------------|
| user_id        | object      |
| creation_date  | object      |
| name           | object      |
| street         | object      |
| state          | object      |
| city           | object      |
| country        | object      |
| birthdate      | object      |
| gender         | object      |
| device_address | object      |
| user_type      | object      |


### Sample Data (First 5 Rows)


| user_id   | creation_date       | name             | street                    | state          | city       | country     | birthdate           | gender   | device_address    | user_type   |
|:----------|:--------------------|:-----------------|:--------------------------|:---------------|:-----------|:------------|:--------------------|:---------|:------------------|:------------|
| user40678 | 2021-03-17 22:56:13 | zion feest       | 14938 west trace side     | new jersey     | birmingham | hong kong   | 1998-04-06 05:29:37 | male     | 17:fb:f2:60:94:4b | basic       |
| user08728 | 2022-10-10 12:53:20 | kattie bergstrom | 4476 west haven fort      | alabama        | irvine     | mayotte     | 2003-05-22 11:16:19 | male     | b0:17:a7:0b:d6:67 | premium     |
| user29759 | 2020-05-20 04:34:44 | aiden corwin     | 59980 north crest chester | north carolina | tampa      | iraq        | 2008-08-29 16:42:05 | female   | 24:f2:0b:88:2f:bd | basic       |
| user16806 | 2021-05-28 07:36:30 | vince gislason   | 541 radial mouth          | illinois       | orlando    | new zealand | 2012-02-09 14:12:37 | male     | a4:f5:fd:fe:07:f9 | basic       |
| user27644 | 2023-03-16 19:25:35 | adele okuneva    | 896 glen bury             | arizona        | reno       | mexico      | 1976-10-13 00:53:54 | male     | ac:80:b3:bc:8d:5f | premium     |


---


## 📊 Table: `customer_user_job`

**Final Row Count:** 5,000

**Column Count:** 4

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|           | Data Type   |
|:----------|:------------|
| user_id   | object      |
| name      | object      |
| job_title | object      |
| job_level | object      |


### Sample Data (First 5 Rows)


| user_id   | name             | job_title   | job_level   |
|:----------|:-----------------|:------------|:------------|
| user40678 | zion feest       | technician  | accounts    |
| user08728 | kattie bergstrom | technician  | solutions   |
| user29759 | aiden corwin     | student     | none        |
| user16806 | vince gislason   | student     | none        |
| user27644 | adele okuneva    | associate   | usability   |


---


## 📊 Table: `enterprise_merchant_data`

**Final Row Count:** 5,000

**Column Count:** 8

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|                | Data Type   |
|:---------------|:------------|
| merchant_id    | object      |
| creation_date  | object      |
| name           | object      |
| street         | object      |
| state          | object      |
| city           | object      |
| country        | object      |
| contact_number | object      |


### Sample Data (First 5 Rows)


| merchant_id   | creation_date       | name             | street                | state          | city         | country                   | contact_number   |
|:--------------|:--------------------|:-----------------|:----------------------|:---------------|:-------------|:--------------------------|:-----------------|
| merchant53971 | 2022-06-28 11:42:04 | whitby group     | 813 north isle bury   | south carolina | boise        | brunei darussalam         | 661.157.5528     |
| merchant56138 | 2020-08-27 20:35:19 | yourmapper       | 54861 springs view    | colorado       | cleveland    | sint maarten (dutch part) | (046)415-8092    |
| merchant31852 | 2021-08-05 12:04:33 | united mayflower | 730 east islands side | virginia       | atlanta      | mali                      | 939.273.0312     |
| merchant63299 | 2021-08-15 19:00:03 | transparagov     | 2619 coves haven      | maine          | indianapolis | chad                      | 1-181-438-5899   |
| merchant16722 | 2023-02-28 20:35:34 | maponics         | 979 north rue borough | wyoming        | boise        | guatemala                 | 494-319-8223     |


---


## 📊 Table: `enterprise_staff_data`

**Final Row Count:** 5,000

**Column Count:** 9

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|                | Data Type   |
|:---------------|:------------|
| staff_id       | object      |
| name           | object      |
| job_level      | object      |
| street         | object      |
| state          | object      |
| city           | object      |
| country        | object      |
| contact_number | object      |
| creation_date  | object      |


### Sample Data (First 5 Rows)


| staff_id     | name              | job_level    | street                  | state        | city           | country             | contact_number   | creation_date       |
|:-------------|:------------------|:-------------|:------------------------|:-------------|:---------------|:--------------------|:-----------------|:--------------------|
| staff0009650 | randall bergstrom | intermediate | 376 land chester        | texas        | omaha          | cook islands        | (138)548-8481    | 2020-09-04 02:33:28 |
| staff0039964 | christian hessel  | intermediate | 945 west camp shire     | new mexico   | san diego      | pakistan            | 393-164-5574     | 2020-08-08 06:50:47 |
| staff0044932 | edgardo fadel     | entry        | 997 expressway town     | rhode island | corpus christi | albania             | 328.133.8850     | 2020-02-10 16:49:18 |
| staff0015819 | jordi gleichner   | entry        | 720 centers burgh       | virginia     | bakersfield    | timor-leste         | 649.258.8115     | 2021-06-11 11:30:29 |
| staff0036616 | price hintz       | intermediate | 1720 north skyway burgh | alabama      | scottsdale     | palestine, state of | 977-698-2305     | 2020-08-10 10:37:55 |


---


## 📊 Table: `marketing_campaign_data`

**Final Row Count:** 10

**Column Count:** 1

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|    | Data Type   |
|:---|:------------|
| campaign_id	campaign_name	campaign_description	discount    | object      |


### Sample Data (First 5 Rows)


| 	campaign_id	campaign_name	campaign_description	discount   |
|:--|
| 0	campaign24776	wouldn't you know it	"""twee retro vinyl single-origin coffee sartorial fanny pack brunch offal health."" - raleigh senger"	1%   |
| 1	campaign33679	could be written on the back of a postage stamp	"""fanny pack gentrify cardigan messenger bag."" - bradley stamm"	1pct   |
| 2	campaign49972	me neither	"""diy pug leggings everyday craft beer cardigan knausgaard +1 crucifix flannel."" - tremayne nader"	10%%   |
| 3	campaign61872	on the huh	"""trust fund pinterest chambray."" - claude aufderhar"	5%   |
| 4	campaign03110	stick a fork in it	"""yolo tumblr yuccie austin."" - jordi kunde"	1percent   |


---


## 📊 Table: `marketing_transactional_campaign_data`

**Final Row Count:** 124,887

**Column Count:** 5

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|                   | Data Type   |
|:------------------|:------------|
| transaction_date  | object      |
| campaign_id       | object      |
| order_id          | object      |
| estimated arrival | object      |
| availed           | int64       |


### Sample Data (First 5 Rows)


| transaction_date   | campaign_id   | order_id                             | estimated arrival   |   availed |
|:-------------------|:--------------|:-------------------------------------|:--------------------|----------:|
| 2023-08-14         | campaign49972 | 0612c246-57f1-40e8-9993-0f8d41992049 | 10days              |         1 |
| 2021-10-12         | campaign46302 | b4c411de-2fd3-4806-91ae-165edc9baa12 | 13days              |         0 |
| 2023-01-30         | campaign29983 | 26de6b40-db2d-40b9-a64c-58736eaf0381 | 3days               |         1 |
| 2022-01-05         | campaign46302 | 26b60a4e-aafe-4b99-bace-034d088a4a53 | 8days               |         1 |
| 2023-09-19         | campaign46302 | 4aab29ae-e610-46bf-92af-199f6f420cee | 14days              |         1 |


---


## 📊 Table: `operations_order_delays`

**Final Row Count:** 200,000

**Column Count:** 2

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|               | Data Type   |
|:--------------|:------------|
| order_id      | object      |
| delay in days | int64       |


### Sample Data (First 5 Rows)


| order_id                             |   delay in days |
|:-------------------------------------|----------------:|
| a0c62902-2728-4b38-bf40-89816ebe183f |               9 |
| 17902aef-e623-48c0-a865-ab427be0f114 |               7 |
| ce47737b-2253-4ff6-90c6-2b5955d1de1e |               7 |
| fc3312cf-f014-4c14-a04d-b26403835c03 |               5 |
| c0053e4c-5b10-4e31-97f6-2d60866e2fbb |               6 |


---
