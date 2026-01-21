# CEU Modern Data Platforms

Data Engineering 2 - Modern Data Platforms: dbt, Snowflake, Databricks, Apache Spark

---

## Installation

### Databricks Setup
1. Sign up for Databricks Free Edition: https://www.databricks.com/learn/free-edition

### Snowflake Setup
2. Register to Snowflake: https://signup.snowflake.com/?trial=student&cloud=aws&region=us-west-2
3. Set up Snowflake tables: https://dbtsetup.nordquant.com/?course=ceu

### dbt Setup
4. Fork this repo as a private repository and clone it to your PC
5. Ensure you have a compatible Python Version: https://docs.getdbt.com/faqs/Core/install-python-compatibility (if you don't, install Python 3.13)
6. Install uv: https://docs.astral.sh/uv/getting-started/installation/
7. Install packages: `uv sync`
8. Activate the virtualenv:
   - Windows (PowerShell): `.\.venv\Scripts\Activate.ps1`
   - Windows (CMD): `.venv\Scripts\activate.bat`
   - WSL (Windows Subsystem for Linux): `source .venv/bin/activate`
   - macOS / Linux: `source .venv/bin/activate`
---

## Starting a dbt Project

Create a dbt project (all platforms):
```sh
dbt init --skip-profile-setup airbnb
```

Once done, drag and drop the `profiles.yml` file you downloaded to the `airbnb` folder.

Try if dbt works:
```sh
dbt debug
```

### Clean Up Example Files

From within the `airbnb` folder, remove the example models that dbt created by default:
```sh
rm -rf models/example
```

Also remove the example model configuration from `dbt_project.yml`. Delete these lines at the end of the file:
```yaml
models:
  airbnb:
    # Config indicated by + and applies to all files under models/example/
    example:
      +materialized: view
```

---

## Data Exploration

Execute these queries in Snowflake:

### Exercise 6: Explore the Data

1. Take a look at the AIRBNB database / schemas / tables (you can use the Snowflake UI for this)
2. Select 10 records from listings - review and understand the data
3. Select 10 records from hosts - review and understand the data
4. Select 10 records from reviews - review and understand the data

<details>
<summary>Solution</summary>

```sql
USE AIRBNB.RAW;

SELECT * FROM RAW_LISTINGS LIMIT 10;
SELECT * FROM RAW_HOSTS LIMIT 10;
SELECT * FROM RAW_REVIEWS LIMIT 10;
```

</details>

### Exercise 7: Answer Questions with SQL

Answer the following questions by writing SQL queries:

1. Which room types are available and how many records does each type have?
2. What is the minimum and maximum value of the column `MINIMUM_NIGHTS`?
3. How many records do we have with the "minimum value" of `MINIMUM_NIGHTS`?
4. What is the minimum and maximum value of `PRICE`?
5. How many positive, negative and neutral reviews are there?
6. What percentage of the hosts is superhost?
7. Are there any reviews for non-existent listings?

<details>
<summary>Solution</summary>

```sql
-- 1. Room types and counts
SELECT ROOM_TYPE, COUNT(*) as NUM_RECORDS FROM RAW_LISTINGS GROUP BY ROOM_TYPE ORDER BY ROOM_TYPE;

-- 2. Min and max MINIMUM_NIGHTS
SELECT MIN(MINIMUM_NIGHTS), MAX(MINIMUM_NIGHTS) FROM RAW_LISTINGS;

-- 3. Records with minimum value of MINIMUM_NIGHTS
SELECT COUNT(*) FROM RAW_LISTINGS WHERE MINIMUM_NIGHTS = 0;

-- 4. Min and max PRICE
SELECT MIN(PRICE), MAX(PRICE) FROM RAW_LISTINGS;

-- 5. Review sentiment counts
SELECT sentiment, COUNT(*) as NUM_RECORDS FROM RAW_REVIEWS WHERE sentiment IS NOT NULL GROUP BY sentiment;

-- 6. Superhost percentage
SELECT SUM(CASE WHEN IS_SUPERHOST='t' THEN 1 ELSE 0 END)/SUM(1)* 100 as SUPERHOST_PERCENT FROM RAW_HOSTS;

-- 7. Reviews for non-existent listings
SELECT r.* FROM RAW_REVIEWS r LEFT JOIN RAW_LISTINGS l ON (r.listing_id = l.id) WHERE l.id IS NULL;
```

</details>

---

## Models

### SRC Listings (Code used in the lesson)

`models/src/src_listings.sql`:

```sql
WITH raw_listings AS (
    SELECT
        *
    FROM
        AIRBNB.RAW.RAW_LISTINGS
)
SELECT
    id AS listing_id,
    name AS listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    price AS price_str,
    created_at,
    updated_at
FROM
    raw_listings
```

### Exercise 1: SRC Reviews

Create a model which builds on top of our `raw_reviews` table.

1. Call the model `models/src/src_reviews.sql`
2. Use a CTE (common table expression) to define an alias called `raw_reviews`. This CTE selects every column from the raw reviews table `AIRBNB.RAW.RAW_REVIEWS`
3. In your final `SELECT`, select every column and record from `raw_reviews` and rename the following columns:
   - `date` to `review_date`
   - `comments` to `review_text`
   - `sentiment` to `review_sentiment`

<details>
<summary>Solution</summary>

```sql
WITH raw_reviews AS (
    SELECT
        *
    FROM
        AIRBNB.RAW.RAW_REVIEWS
)
SELECT
    listing_id,
    date AS review_date,
    reviewer_name,
    comments AS review_text,
    sentiment AS review_sentiment
FROM
    raw_reviews
```

</details>

### Exercise 2: SRC Hosts

Create a model which builds on top of our `raw_hosts` table.

1. Call the model `models/src/src_hosts.sql`
2. Use a CTE (common table expression) to define an alias called `raw_hosts`. This CTE selects every column from the raw hosts table `AIRBNB.RAW.RAW_HOSTS`
3. In your final `SELECT`, select every column and record from `raw_hosts` and rename the following columns:
   - `id` to `host_id`
   - `name` to `host_name`

<details>
<summary>Solution</summary>

```sql
WITH raw_hosts AS (
    SELECT
        *
    FROM
        AIRBNB.RAW.RAW_HOSTS
)
SELECT
    id AS host_id,
    NAME AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    raw_hosts
```

</details>

---

## Sources

Create a new file called `models/sources.yml`.
Add the `listings` source that points to the `raw_listings` table in the `raw` schema:

```yaml
sources:
  - name: airbnb
    schema: raw
    tables:
      - name: listings
        identifier: raw_listings
```

### Exercise 3: Add Hosts and Reviews Sources

Add the `hosts` and `reviews` sources to your `models/sources.yml` file.
Both should point to their respective raw tables (`raw_hosts` and `raw_reviews`) in the `raw` schema.

<details>
<summary>Solution</summary>

```yaml
sources:
  - name: airbnb
    schema: raw
    tables:
      - name: listings
        identifier: raw_listings

      - name: hosts
        identifier: raw_hosts

      - name: reviews
        identifier: raw_reviews
```

</details>

---

## Incremental Models

The `models/fct/fct_reviews.sql` model:
```sql
{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}
WITH src_reviews AS (
  SELECT * FROM {{ ref('src_reviews') }}
)
SELECT * FROM src_reviews
WHERE review_text is not null

{% if is_incremental() %}
  AND review_date > (select max(review_date) from {{ this }})
{% endif %}
```

Run the model:
```sh
dbt run --select fct_reviews
```

Get every review for listing _3176_ (in Snowflake):
```sql
SELECT * FROM "AIRBNB"."DEV"."FCT_REVIEWS" WHERE listing_id=3176;
```

Add a new record to the _RAW_ table (in Snowflake):
```sql
INSERT INTO "AIRBNB"."RAW"."RAW_REVIEWS"
VALUES (3176, CURRENT_TIMESTAMP(), 'Zoltan', 'excellent stay!', 'positive');
```

Only add the new record:
```sh
dbt run
```

Or make a full-refresh:
```sh
dbt run --full-refresh
```

---
## Logs

Take a look at the `logs` folder (in the `airbnb` folder) to see what SQLs were executed.

Also take a look at:

 * `target/compiled`
 * `target/run`

---

---
### A loaded_at fields

It's always a good idea to add a `loaded_at` field the stores the time of record creation to fct_reviews

In `fct_reviews`, change
```
  SELECT *, current_timestamp()() AS loaded_at FROM {{ ref('src_reviews') }} -- Adding loaded_at column
```

Then materialize only this model:
```
dbt run --full-refresh --select fct_reviews
```

## Visualizing our graph
Execute:
```
dbt docs generate
dbt docs serve
```

---

## Source Freshness Testing

Add freshness configuration to the `reviews` source in `models/sources.yml`:

```yaml
      - name: reviews
        identifier: raw_reviews
        config:
          loaded_at_field: load
          freshness:
            warn_after: {count: 1, period: day}
```

Check source freshness:
```sh
dbt source freshness
```

Try it with a one-minute tolerance
```
            warn_after: {count: 1, period: minute}
```
---

## Cleansed Models

### DIM Listings Cleansed (Code used in the lesson)

`models/dim/dim_listings_cleansed.sql`:

```sql
WITH src_listings AS (
    SELECT * FROM {{ ref('src_listings') }}
)
SELECT
  listing_id,
  listing_name,
  room_type,
  CASE
    WHEN minimum_nights = 0 THEN 1
    ELSE minimum_nights
  END AS minimum_nights,
  host_id,
  REPLACE(
    price_str,
    '$'
  ) :: NUMBER(
    10,
    2
  ) AS price,
  created_at,
  updated_at
FROM
  src_listings
```

Materialize only `dim` models: _(`-s` is short for `--select`)
```
dbt run -s dim
```

### Exercise 4: DIM Hosts Cleansed

Create a new model in the `models/dim/` folder called `dim_hosts_cleansed.sql`.
Use a CTE to reference the `src_hosts` model.
SELECT every column and every record, and add a cleansing step to `host_name`:
- If `host_name` is not null, keep the original value
- If `host_name` is null, replace it with the value `'Anonymous'`
- Use the `NVL(column_name, default_null_value)` function

<details>
<summary>Solution</summary>

```sql
WITH src_hosts AS (
    SELECT
        *
    FROM
        {{ ref('src_hosts') }}
)
SELECT
    host_id,
    NVL(
        host_name,
        'Anonymous'
    ) AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    src_hosts
```

</details>

### Exercise 5: DIM Listings with Hosts

Create a new model in the `models/dim/` folder called `dim_listings_w_hosts.sql`.
Join `dim_listings_cleansed` with `dim_hosts_cleansed` to create a denormalized view that includes host information alongside listing data.
- Use a LEFT JOIN on `host_id`
- Include all listing fields plus `host_name` and `is_superhost` (renamed to `host_is_superhost`)
- For `updated_at`, use the `GREATEST()` function to get the most recent update from either table

<details>
<summary>Solution</summary>

```sql
WITH
l AS (
    SELECT
        *
    FROM
        {{ ref('dim_listings_cleansed') }}
),
h AS (
    SELECT *
    FROM {{ ref('dim_hosts_cleansed') }}
)

SELECT
    l.listing_id,
    l.listing_name,
    l.room_type,
    l.minimum_nights,
    l.price,
    l.host_id,
    h.host_name,
    h.is_superhost as host_is_superhost,
    l.created_at,
    GREATEST(l.updated_at, h.updated_at) as updated_at
FROM l
LEFT JOIN h ON (h.host_id = l.host_id)
```

</details>

### Exercise 6
Take a look at your pipeline by generating the docs and starting the docs server 
<details>
<summary>Solution</summary>
```
dbt docs generate
dbt docs serve
```
</details>

---

## Materializations

### Project-level Materialization

Set `src` models to `ephemeral` and `dim` models to `view` in `dbt_project.yml`:

```yaml
models:
  airbnb:
    src:
      +materialized: ephemeral
    dim:
      +materialized: view # This is default, but let's make it explicit
```

After setting ephemeral materialization, drop the existing src views in Snowflake:
```sql
DROP VIEW AIRBNB.DEV.SRC_HOSTS;
DROP VIEW AIRBNB.DEV.SRC_LISTINGS;
DROP VIEW AIRBNB.DEV.SRC_REVIEWS;
```

### Model-level Materialization

Set `dim_listings_w_hosts` to `table` materialization by adding a config block to the model:

`models/dim/dim_listings_w_hosts.sql`:
```sql
{{
  config(
    materialized = 'table'
  )
}}
WITH
l AS (
...
```

## Seeds

Sometimes you have smaller datasets that are not added to Snowflake by external systems and you want to add them manually. _Seeds_ are here to the rescue:

1) Explore the `seed` folder
2) Run `dbt seeds`
3) Check for the table on the snowflake UI

### Exercise: Full Moon Reviews Mart

Create a mart model that analyzes whether reviews were written during a full moon. This exercise combines your `fct_reviews` model with the `seed_full_moon_dates` seed data.

**Task:** Create `models/mart/mart_fullmoon_reviews.sql` that:

1. References both `fct_reviews` and `seed_full_moon_dates` using the `{{ ref() }}` function
2. Joins reviews with full moon dates to determine if each review was written the day after a full moon
3. Adds a new column `is_full_moon` that contains:
   - `'full moon'` if the review was written the day after a full moon
   - `'not full moon'` otherwise
4. Configure the model as a `table` materialization

**Hints:**
- Use CTEs to reference each model separately
- Snowflake date functions you'll need:
  - `TO_DATE(timestamp_column)` - Converts a timestamp to a date (strips the time component)
  - `DATEADD(DAY, 1, date_column)` - Adds 1 day to a date (we want reviews from the day *after* the full moon)
- The join condition should match the review date with the day after the full moon date

**Validation:** After running `dbt run --select mart_fullmoon_reviews`, query the result in Snowflake:
```sql
SELECT is_full_moon, COUNT(*) as review_count
FROM AIRBNB.DEV.MART_FULLMOON_REVIEWS
GROUP BY is_full_moon;
```

<details>
<summary>Solution</summary>

```sql
{{ config(
  materialized = 'table',
) }}

WITH fct_reviews AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),
full_moon_dates AS (
    SELECT * FROM {{ ref('seed_full_moon_dates') }}
)

SELECT
  r.*,
  CASE
    WHEN fm.full_moon_date IS NULL THEN 'not full moon'
    ELSE 'full moon'
  END AS is_full_moon
FROM
  fct_reviews
  r
  LEFT JOIN full_moon_dates
  fm
  ON (TO_DATE(r.review_date) = DATEADD(DAY, 1, fm.full_moon_date))
```

</details>

### Exercise: Full Moon Sentiment Analysis

Create an **analysis** to investigate whether full moons affect review sentiment. Analyses are SQL files in the `analyses/` folder that are compiled but not materialized - they're useful for ad-hoc queries and reporting.

**Task:** Create `analyses/fullmoon_sentiment.sql` that:

1. References the `mart_fullmoon_reviews` model
2. Filters out neutral sentiments (only keep `'positive'` and `'negative'`)
3. For each `is_full_moon` category, calculate:
   - `positive_count` - number of positive reviews
   - `total_count` - total number of reviews (positive + negative)
   - `positive_percentage` - percentage of positive reviews (e.g., 85.5 for 85.5%)
4. Returns two rows: one for `'full moon'` and one for `'not full moon'`

**Hints:**
- Use conditional aggregation: `SUM(CASE WHEN condition THEN 1 ELSE 0 END)` counts matching rows
- Snowflake integer division truncates decimals - multiply by `100.0` to get a percentage
- Use `ROUND(value, 2)` to round to 2 decimal places for cleaner output

**Run the analysis:**
```sh
dbt show --select fullmoon_sentiment
```

<details>
<summary>Solution</summary>

```sql
WITH fullmoon_reviews AS (
    SELECT * FROM {{ ref('mart_fullmoon_reviews') }}
)
SELECT
    is_full_moon,
    SUM(CASE WHEN review_sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_count,
    COUNT(*) AS total_count,
    ROUND(SUM(CASE WHEN review_sentiment = 'positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS positive_percentage
FROM
    fullmoon_reviews
WHERE
    review_sentiment != 'neutral'
GROUP BY
    is_full_moon
ORDER BY
    is_full_moon
```

</details>

---

## Snapshots
Snapshots implement tracking of slowly changing dimensions: (see [Slowly changing dimension — Type 2 (SCD2)](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2))

## Snapshots for listing
The contents of `snapshots/snapshots.yml`:
```yaml
snapshots:
  - name: scd_raw_listings
    relation: source('airbnb', 'listings')
    config:
      unique_key: id
      strategy: timestamp
      updated_at: updated_at
      hard_deletes: invalidate
```

Materialize the snapshot:
```
dbt snapshot
```

Take a look at a single record:
```
SELECT * FROM AIRBNB.DEV.SCD_RAW_LISTINGS WHERE ID=3176;
```

### Updating the table
```sql
SELECT * FROM AIRBNB.RAW.RAW_LISTINGS WHERE ID=3176;
```

```sql
UPDATE AIRBNB.RAW.RAW_LISTINGS SET MINIMUM_NIGHTS=30,
    updated_at=CURRENT_TIMESTAMP() WHERE ID=3176;
```

```sql
SELECT * FROM AIRBNB.RAW.RAW_LISTINGS WHERE ID=3176;
```

Run `dbt snapshot` again

Let's see the changes:
```
SELECT * FROM AIRBNB.DEV.SCD_RAW_LISTINGS WHERE ID=3176;
```


### Building everything with the same command
```
dbt build
```

### Exercise
1) Create a snapshot for `raw_hosts`
2) run it
3) update raw_hosts
4) run snapshot again
5) validate the change in the snapshot
<details>
<summary>Solution</summary>

Add this to `snapshots/snapshots.yml`:
```yaml
snapshots:
  - name: scd_raw_hosts
    relation: source('airbnb', 'hosts')
    config:
      unique_key: id
      strategy: timestamp
      updated_at: updated_at
      hard_deletes: invalidate
```
</details>

## Tests

