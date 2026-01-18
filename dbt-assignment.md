# dbt Assignment: AirStats Capstone Project

Build a dbt project analyzing global airport data on Snowflake.

---

## Prerequisites

1. **Snowflake account** - You should already have access from the course setup
2. **dbt installed** - Should be set up from the main course exercises

---

## Part 1: Project Setup

### Step 1: Initialize the dbt Project

* Fork this repository: https://github.com/zoltanctoth/ceu-dbt-assignment-repo 
* Execute setup steps if working locally (see `README.md` in your fork)
* Add your student id to `README.md`
* Create a new dbt project called `airstats`.

### Step 2: Configure the Connection

Copy the `profiles.yml` from the `airbnb` project to the `airstats` folder:

```sh
cp airbnb/profiles.yml airstats/profiles.yml
```

Now edit `airstats/profiles.yml` and make these changes:

**Before (airbnb configuration):**
```yaml
airbnb:
  outputs:
    dev:
      type: snowflake
      account: "..."
      user: dbt
      role: TRANSFORM
      private_key: "..."
      private_key_passphrase: q
      database: AIRBNB
      schema: DEV
      threads: 1
      warehouse: COMPUTE_WH
  target: dev
```

**After (airstats configuration):**
```yaml
airstats:
  outputs:
    dev:
      type: snowflake
      account:  "..."
      user: dbt
      role: TRANSFORM
      private_key: "..."
      private_key_passphrase: q
      database: AIRSTATS
      schema: DEV
      threads: 1
      warehouse: COMPUTE_WH
  target: dev
```

**Summary of changes:**
1. Change the profile name from `airbnb` to `airstats` (first line)
2. Change the database from `AIRBNB` to `AIRSTATS`

### Step 3: Verify the Connection

```sh
cd airstats
dbt debug
```

You should see "All checks passed!" if the connection is configured correctly.

### Step 4: Clean Up Example Files

Remove the example models that dbt created by default:

```sh
rm -rf models/example
```

Also remove the example model configuration from `dbt_project.yml`. Delete these lines at the end of the file:

```yaml
models:
  airstats:
    example:
      +materialized: view
```

---


### Part 2: Data Exploration (Optional)

Before building models, explore the data in Snowflake, here are a few SQLs to get you started

```sql
USE AIRSTATS.RAW;

-- Check the airports table
SELECT * FROM airports LIMIT 10;

-- Count airports by type
SELECT type, COUNT(*) as count
FROM airports
GROUP BY type
ORDER BY count DESC;

-- Check the runways table
SELECT * FROM runways LIMIT 10;

-- Check the comments table
SELECT * FROM airport_comments LIMIT 10;
```

---
## Part 3: Define Sources

The AIRSTATS database has been set up in Snowflake with the following tables in the `RAW` schema:

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `airports` | Global airport data (~72K rows) | `id`, `ident`, `type`, `name`, `iso_country` |
| `airport_comments` | User comments about airports | `id`, `airport_ref`, `airport_ident`, `date` |
| `runways` | Runway information (~44K rows) | `id`, `airport_ref`, `airport_ident`, `closed` |

### Exercise 1: Create the Sources File

Create a new file `models/sources.yml` that defines these three source tables.

**Requirements:**
1. The source name should be `airstats`
2. The database should be `AIRSTATS`
3. The schema should be `RAW`
4. Define all three tables: `airports`, `airport_comments`, and `runways`

### Verify Your Sources

After creating the sources file, you can verify it works by running:

```sh
dbt compile
```

If there are no errors, your sources are configured correctly.

## Next Steps

In the following exercises, you will:
1. Create staging models (src layer) that rename and select columns
2. Create dimension models that filter and aggregate data
3. Create a final analytics mart that joins everything together

_I'm going to send you the instructions by EOD 20 Jan 2026._

### Submission

* Create a private git repository and invite `zoltanctoth` and `nai-coder` as collaborator
* 
Submission deadline: 11:59 pm, Sunday 25 January, 2026
Submission through Moodle.

