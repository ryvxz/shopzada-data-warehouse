# Data Ingestion and Quality Report

**Generated On:** 2025-12-18 12:38:05

**Total Tables Processed:** 13


---


## 📊 Table: `business_product_list`

**Final Row Count:** 750

**Column Count:** 4

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|              | Data Type   |
|:-------------|:------------|
| product_id   | object      |
| product_name | object      |
| product_type | object      |
| price        | float64     |


### Sample Data (First 5 Rows)


| product_id   | product_name                   | product_type        |   price |
|:-------------|:-------------------------------|:--------------------|--------:|
| product16794 | grandmas swedish thin pancakes | readymade_breakfast |   12.81 |
| product61957 | chili jack oven omelet         | readymade_breakfast |    9.95 |
| product23890 | baked pears                    | readymade_breakfast |   10.04 |
| product52912 | best buttermilk pancakes       | readymade_breakfast |    5.83 |
| product56387 | blackberry breakfast bars      | readymade_breakfast |   14    |


---

### Some Unique Values


### Column product_type unique values:

['readymade_breakfast' 'readymade_lunch' 'readymade_dinner' 'accessories'
 'kitchenware' 'toys and entertainment' 'grocery' 'apparel' 'furniture'
 'health and hygiene' 'stationary' 'tools' 'jewelry' 'technology'
 'electronics and technology' 'sports' 'cosmetics'
 'stationary and school supplies' 'school supplies' 'music' 'others'
 'cleaning materials' 'cosmetic' 'appliances' 'toolss' 'none']




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

### Some Unique Values


### Column issuing_bank unique values:

['bpi' 'bdo' 'chinabank' 'metrobank' 'mayabank' 'robinsonsbank'
 'securitybank' 'eastwest']




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

### Some Unique Values


### Column gender unique values:

['male' 'female']



### Column user_type unique values:

['basic' 'premium' 'verified']




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

### Some Unique Values


### Column job_title unique values:

['technician' 'student' 'associate' 'liaison' 'director' 'producer'
 'executive' 'strategist' 'planner' 'facilitator' 'designer'
 'orchestrator' 'developer' 'manager' 'consultant' 'engineer' 'assistant'
 'supervisor' 'representative' 'specialist' 'analyst' 'coordinator'
 'architect' 'agent' 'officer' 'administrator']



### Column job_level unique values:

['accounts' 'solutions' 'none' 'usability' 'mobility' 'operations'
 'security' 'assurance' 'markets' 'implementation' 'paradigm' 'creative'
 'response' 'accountability' 'interactions' 'directives' 'intranet'
 'configuration' 'integration' 'program' 'identity' 'factors' 'division'
 'research' 'brand' 'tactics' 'quality' 'data' 'optimization' 'metrics'
 'infrastructure' 'applications' 'branding' 'web' 'communications'
 'marketing' 'functionality' 'group']




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

### Some Unique Values



## 📊 Table: `enterprise_order_with_merchant_data`

**Final Row Count:** 500,000

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

### Some Unique Values



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

### Some Unique Values


### Column job_level unique values:

['intermediate' 'entry' 'senior']




## 📊 Table: `marketing_campaign_data`

**Final Row Count:** 10

**Column Count:** 4

### Columns with Remaining Null Values

No null values detected (or they were handled by the reader/standardization).


### Final Column Data Types

|                      | Data Type   |
|:---------------------|:------------|
| campaign_id          | object      |
| campaign_name        | object      |
| campaign_description | object      |
| discount             | object      |


### Sample Data (First 5 Rows)


| campaign_id   | campaign_name                                   | campaign_description                                                                               | discount   |
|:--------------|:------------------------------------------------|:---------------------------------------------------------------------------------------------------|:-----------|
| campaign24776 | wouldn't you know it                            | "twee retro vinyl single-origin coffee sartorial fanny pack brunch offal health." - raleigh senger | 1%         |
| campaign33679 | could be written on the back of a postage stamp | "fanny pack gentrify cardigan messenger bag." - bradley stamm                                      | 1pct       |
| campaign49972 | me neither                                      | "diy pug leggings everyday craft beer cardigan knausgaard +1 crucifix flannel." - tremayne nader   | 10%%       |
| campaign61872 | on the huh                                      | "trust fund pinterest chambray." - claude aufderhar                                                | 5%         |
| campaign03110 | stick a fork in it                              | "yolo tumblr yuccie austin." - jordi kunde                                                         | 1percent   |


---

### Some Unique Values


### Column campaign_id unique values:

['campaign24776' 'campaign33679' 'campaign49972' 'campaign61872'
 'campaign03110' 'campaign46302' 'campaign11190' 'campaign53595'
 'campaign29983' 'campaign52447']



### Column campaign_name unique values:

["wouldn't you know it" 'could be written on the back of a postage stamp'
 'me neither' 'on the huh' 'stick a fork in it' 'you must be new here'
 'mind your own beeswax' 'how do i get to the train station'
 'would it hurt' 'pound for pound']



### Column campaign_description unique values:

['"twee retro vinyl single-origin coffee sartorial fanny pack brunch offal health." - raleigh senger'
 '"fanny pack gentrify cardigan messenger bag." - bradley stamm'
 '"diy pug leggings everyday craft beer cardigan knausgaard +1 crucifix flannel." - tremayne nader'
 '"trust fund pinterest chambray." - claude aufderhar'
 '"yolo tumblr yuccie austin." - jordi kunde'
 '"craft beer xoxo hella tacos chillwave cred organic letterpress disrupt artisan." - rodrick lebsack'
 '"street shoreditch viral before they sold out yr ramps skateboard skateboard bitters pabst." - brendan miller'
 '"craft beer venmo lomo fixie readymade marfa." - benny bogan'
 '"vegan migas ramps keytar wolf cray kickstarter five dollar toast." - adeline brakus'
 '"semiotics biodiesel everyday craft beer etsy semiotics keffiyeh meditation single-origin coffee." - bernadette pollich']



### Column discount unique values:

['1%' '1pct' '10%%' '5%' '1percent' '10pct' '20pct']




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

### Some Unique Values


### Column campaign_id unique values:

['campaign49972' 'campaign46302' 'campaign29983' 'campaign03110'
 'campaign61872' 'campaign24776' 'campaign33679' 'campaign53595'
 'campaign52447' 'campaign11190']



### Column estimated arrival unique values:

['10days' '13days' '3days' '8days' '14days' '11days' '7days' '9days'
 '4days' '5days' '12days' '6days' '15days']



### Column availed unique values:

[1 0]




## 📊 Table: `operations_line_item_data_prices`

**Final Row Count:** 1,958,172

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

### Some Unique Values



## 📊 Table: `operations_line_item_data_products`

**Final Row Count:** 1,832,083

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

### Some Unique Values



## 📊 Table: `operations_order_data`

**Final Row Count:** 127,790

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

### Some Unique Values


### Column estimated arrival unique values:

['3days' '7days' '4days' '12days' '10days' '11days' '5days' '13days'
 '9days' '8days' '14days' '6days' '15days']




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

### Some Unique Values


### Column delay in days unique values:

[9 7 5 6 3 0 1 2 8 4]


