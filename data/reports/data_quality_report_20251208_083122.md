# Data Ingestion and Quality Report

**Generated On:** 2025-12-08 08:31:22

**Total Tables Processed:** 22


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


## 📊 Table: `enterprise_order_with_merchant_data1`

**Final Row Count:** 100,000

**Column Count:** 3

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|             | Data Type   |
|:------------|:------------|
| order_id    | object      |
| merchant_id | object      |
| staff_id    | object      |


### Sample Data (First 5 Rows)


| order_id                             | merchant_id   | staff_id     |
|:-------------------------------------|:--------------|:-------------|
| eac330c0-457a-4faa-b15a-52a3c440e7f3 | merchant58557 | staff0027757 |
| e1beaf61-e687-4e70-bdd4-3ea338139a0b | merchant0605  | staff0039068 |
| 0612c246-57f1-40e8-9993-0f8d41992049 | merchant22282 | staff0058495 |
| a800f0d9-47d8-455b-b096-622e76156705 | merchant39307 | staff0038632 |
| b4c411de-2fd3-4806-91ae-165edc9baa12 | merchant26962 | staff0035568 |


---


## 📊 Table: `enterprise_order_with_merchant_data2`

**Final Row Count:** 200,000

**Column Count:** 3

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|             | Data Type   |
|:------------|:------------|
| order_id    | object      |
| merchant_id | object      |
| staff_id    | object      |


### Sample Data (First 5 Rows)


| order_id                             | merchant_id   | staff_id     |
|:-------------------------------------|:--------------|:-------------|
| 848f9cad-c4d3-4822-83e9-322ae73261c3 | merchant9789  | staff0020354 |
| be97922b-20fb-4244-9e25-bac98b209668 | merchant43424 | staff0061355 |
| dc905240-5fcd-45e4-8077-be831e1f0263 | merchant23900 | staff0038795 |
| 11cd8029-61f2-4bc8-a148-cb1a4afd8b57 | merchant58797 | staff0023844 |
| 69c9e6fc-326e-4f2d-98c6-5360b91cfb84 | merchant41587 | staff0022537 |


---


## 📊 Table: `enterprise_order_with_merchant_data3`

**Final Row Count:** 200,000

**Column Count:** 3

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|             | Data Type   |
|:------------|:------------|
| order_id    | object      |
| merchant_id | object      |
| staff_id    | object      |


### Sample Data (First 5 Rows)


| order_id                             | merchant_id   | staff_id     |
|:-------------------------------------|:--------------|:-------------|
| e8bafe72-d506-4437-b357-f38682353238 | merchant13004 | staff0009879 |
| eae5e846-c16b-4959-9f90-331c355951ff | merchant17176 | staff0045676 |
| 913f901b-d488-49be-bcaf-86fcd581a528 | merchant0855  | staff0031852 |
| 8479d795-6c4d-41d0-a0c7-4ca9c3c3ca31 | merchant40576 | staff0052472 |
| 2aaae5b3-c858-4d05-b92f-cd2085201c66 | merchant32289 | staff0031305 |


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


## 📊 Table: `operations_line_item_data_prices1`

**Final Row Count:** 489,131

**Column Count:** 3

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|          | Data Type   |
|:---------|:------------|
| order_id | object      |
| price    | float64     |
| quantity | object      |


### Sample Data (First 5 Rows)


| order_id                             |   price | quantity   |
|:-------------------------------------|--------:|:-----------|
| 8d8acbac-ccbb-4609-a978-98dee3ac3088 |   12.81 | 6px        |
| 59e4c308-5262-486d-9f1a-12b1278e3c44 |   14    | 6pieces    |
| 30fc1b76-c488-4077-8d70-3236e3afc990 |    6.78 | 4pcs       |
| 6b273d38-b472-4bb2-8b74-a26705c707fa |    8.72 | 4px        |
| 6b273d38-b472-4bb2-8b74-a26705c707fa |    8.83 | 5pieces    |


---


## 📊 Table: `operations_line_item_data_prices2`

**Final Row Count:** 489,269

**Column Count:** 3

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|          | Data Type   |
|:---------|:------------|
| order_id | object      |
| price    | float64     |
| quantity | object      |


### Sample Data (First 5 Rows)


| order_id                             |   price | quantity   |
|:-------------------------------------|--------:|:-----------|
| f1c591a5-fd7c-498f-ae4e-2739334878ce |   38.07 | 4pc        |
| 8a720341-a145-4a7c-b2d5-ac3c6e0d8da8 |   38.07 | 4pcs       |
| 8a720341-a145-4a7c-b2d5-ac3c6e0d8da8 |   38.07 | 4pcs       |
| fd365f1c-9c25-4544-b4b2-fd9f55d3e6e2 |   38.07 | 2pcs       |
| fd365f1c-9c25-4544-b4b2-fd9f55d3e6e2 |   38.07 | 4pieces    |


---


## 📊 Table: `operations_line_item_data_prices3`

**Final Row Count:** 979,791

**Column Count:** 3

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|          | Data Type   |
|:---------|:------------|
| order_id | object      |
| price    | float64     |
| quantity | object      |


### Sample Data (First 5 Rows)


| order_id                             |   price | quantity   |
|:-------------------------------------|--------:|:-----------|
| dc70f978-3a6b-4baf-b97e-4a88690a34fe |   42.13 | 8pc        |
| dc70f978-3a6b-4baf-b97e-4a88690a34fe |   42.13 | 5piece     |
| 3556b3e7-046a-4148-b1cb-d86ee7ee5421 |   42.13 | 10pc       |
| 3556b3e7-046a-4148-b1cb-d86ee7ee5421 |   42.13 | 5pcs       |
| 29b63149-3b3c-4309-a751-7d0a563f7c12 |   40.87 | 5pc        |


---


## 📊 Table: `operations_line_item_data_products1`

**Final Row Count:** 435,182

**Column Count:** 3

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|              | Data Type   |
|:-------------|:------------|
| order_id     | object      |
| product_name | object      |
| product_id   | object      |


### Sample Data (First 5 Rows)


| order_id                             | product_name                   | product_id   |
|:-------------------------------------|:-------------------------------|:-------------|
| 8d8acbac-ccbb-4609-a978-98dee3ac3088 | grandmas swedish thin pancakes | product16794 |
| 59e4c308-5262-486d-9f1a-12b1278e3c44 | blackberry breakfast bars      | product56387 |
| 30fc1b76-c488-4077-8d70-3236e3afc990 | moms cheat doughnuts           | product26612 |
| 6b273d38-b472-4bb2-8b74-a26705c707fa | baked swiss cheese omelet      | product17344 |
| 6b273d38-b472-4bb2-8b74-a26705c707fa | egg flowers                    | product07816 |


---


## 📊 Table: `operations_line_item_data_products2`

**Final Row Count:** 447,455

**Column Count:** 3

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|              | Data Type   |
|:-------------|:------------|
| order_id     | object      |
| product_name | object      |
| product_id   | object      |


### Sample Data (First 5 Rows)


| order_id                             | product_name   | product_id   |
|:-------------------------------------|:---------------|:-------------|
| f1c591a5-fd7c-498f-ae4e-2739334878ce | clock          | product14601 |
| 8a720341-a145-4a7c-b2d5-ac3c6e0d8da8 | book of jokes  | product63300 |
| fd365f1c-9c25-4544-b4b2-fd9f55d3e6e2 | book of jokes  | product63300 |
| 484f9ec5-d747-4a02-a9c9-02b249aa8f5c | gaming gards   | product14660 |
| 7489e6c5-8e1e-4ec0-91c7-e2d1308a5ef1 | gaming gards   | product14660 |


---


## 📊 Table: `operations_line_item_data_products3`

**Final Row Count:** 949,447

**Column Count:** 3

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|              | Data Type   |
|:-------------|:------------|
| order_id     | object      |
| product_name | object      |
| product_id   | object      |


### Sample Data (First 5 Rows)


| order_id                             | product_name                       | product_id   |
|:-------------------------------------|:-----------------------------------|:-------------|
| dc70f978-3a6b-4baf-b97e-4a88690a34fe | swedish cucumber salad pressgurka  | product20506 |
| 3556b3e7-046a-4148-b1cb-d86ee7ee5421 | swedish cucumber salad pressgurka  | product20506 |
| 29b63149-3b3c-4309-a751-7d0a563f7c12 | savory pita chips                  | product34708 |
| e2cf9c27-5120-4129-928c-0a436ea9a522 | savory pita chips                  | product34708 |
| a7d54380-1923-4b9e-932d-112bcdb444bf | sweet onion and mashed potato bake | product62552 |


---


## 📊 Table: `operations_order_data_20200101-20200701`

**Final Row Count:** 63,024

**Column Count:** 4

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|                   | Data Type   |
|:------------------|:------------|
| order_id          | object      |
| user_id           | object      |
| estimated arrival | object      |
| transaction_date  | object      |


### Sample Data (First 5 Rows)


| order_id                             | user_id   | estimated arrival   | transaction_date   |
|:-------------------------------------|:----------|:--------------------|:-------------------|
| 0248bb48-d0d0-4a11-a158-f620640a757a | user16283 | 13days              | 2020-06-04         |
| 14473b59-6c4f-429e-89ef-ce920b209df3 | user08179 | 8days               | 2020-04-07         |
| 62b8fabf-4e0a-4383-8b3a-7276c77f9e8c | user55688 | 10days              | 2020-05-07         |
| 96a01cee-071d-4e10-a9e4-56a31b82a9d5 | user34967 | 8days               | 2020-06-05         |
| 479b1a6c-3a76-41a7-b15d-71acd4045080 | user07130 | 15days              | 2020-05-24         |


---


## 📊 Table: `operations_order_data_20200701-20211001`

**Final Row Count:** 159,725

**Column Count:** 4

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|                   | Data Type   |
|:------------------|:------------|
| order_id          | object      |
| user_id           | object      |
| estimated arrival | object      |
| transaction_date  | object      |


### Sample Data (First 5 Rows)


| order_id                             | user_id   | estimated arrival   | transaction_date   |
|:-------------------------------------|:----------|:--------------------|:-------------------|
| e1beaf61-e687-4e70-bdd4-3ea338139a0b | user36309 | 13days              | 2021-01-01         |
| 4cad07a9-dca9-47a0-86f1-f83671d05260 | user46704 | 5days               | 2021-09-22         |
| 8750f8c5-380c-4bbd-bb54-8a2b5e5958ac | user32071 | 10days              | 2020-07-06         |
| 3d77b756-0f11-4145-9fac-8f229611ac39 | user47160 | 3days               | 2021-09-25         |
| 3312851e-28dc-4508-8003-09ba304ce9e2 | user15307 | 11days              | 2020-07-01         |


---


## 📊 Table: `operations_order_data_20211001-20220101`

**Final Row Count:** 32,288

**Column Count:** 4

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|                   | Data Type   |
|:------------------|:------------|
| order_id          | object      |
| user_id           | object      |
| estimated arrival | object      |
| transaction_date  | object      |


### Sample Data (First 5 Rows)


| order_id                             | user_id   | estimated arrival   | transaction_date   |
|:-------------------------------------|:----------|:--------------------|:-------------------|
| b4c411de-2fd3-4806-91ae-165edc9baa12 | user28531 | 13days              | 2021-10-12         |
| d42d7e7a-71bd-43aa-8bed-49d52b74d4eb | user56935 | 13days              | 2021-10-15         |
| 84295de8-90c2-4e67-a1d9-b1ffab45623f | user11824 | 13days              | 2021-10-11         |
| 74a59808-b7f5-4dc1-b441-a8e0e15b49b6 | user51578 | 15days              | 2021-10-22         |
| b3965e7d-23d7-475b-93f0-13b49a999030 | user51925 | 9days               | 2021-10-09         |


---


## 📊 Table: `operations_order_data_20221201-20230601`

**Final Row Count:** 63,744

**Column Count:** 4

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|                   | Data Type   |
|:------------------|:------------|
| order_id          | object      |
| user_id           | object      |
| estimated arrival | object      |
| transaction_date  | object      |


### Sample Data (First 5 Rows)


| order_id                             | user_id   | estimated arrival   | transaction_date   |
|:-------------------------------------|:----------|:--------------------|:-------------------|
| 26de6b40-db2d-40b9-a64c-58736eaf0381 | user36943 | 3days               | 2023-01-30         |
| 71ef25bd-1146-4ac9-b371-ede0cda08093 | user10211 | 7days               | 2023-04-13         |
| d06a6ad2-b254-4cc6-8397-4c9bf1a2823e | user28839 | 3days               | 2023-04-05         |
| 8bfbd31b-5a72-4c80-9e95-9168e9d822e3 | user00089 | 4days               | 2023-04-07         |
| 3fab0712-4542-4779-89cc-29518063ce8f | user63283 | 12days              | 2023-04-18         |


---


## 📊 Table: `operations_order_data_20230601-20240101`

**Final Row Count:** 64,046

**Column Count:** 4

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|                   | Data Type   |
|:------------------|:------------|
| order_id          | object      |
| user_id           | object      |
| estimated arrival | object      |
| transaction_date  | object      |


### Sample Data (First 5 Rows)


| order_id                             | user_id   | estimated arrival   | transaction_date   |
|:-------------------------------------|:----------|:--------------------|:-------------------|
| 0612c246-57f1-40e8-9993-0f8d41992049 | user24284 | 10days              | 2023-08-14         |
| 74e7c07a-d266-492f-a8f7-22987013d7e1 | user47513 | 15days              | 2023-10-30         |
| 4aab29ae-e610-46bf-92af-199f6f420cee | user50778 | 14days              | 2023-09-19         |
| 89c3c238-2ae5-4883-bba5-08e23dbedee3 | user44062 | 8days               | 2023-07-11         |
| 03c25b03-a7cc-4758-8490-cdd5dd492646 | user48153 | 15days              | 2023-07-21         |


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
