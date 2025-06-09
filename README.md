
# Spond Data Engineering Case Solution 🎯

> _Setup requires Java 17 and Gradle._

This is **my version** of the solution for the Spond Data Engineering case.  
The codebase is organized into the following modules:

- `ingestion`: Handles loading and parsing of input data.
- `validation`: Performs referential integrity checks and other data quality validations.
- `modeling`: Prepares analytical models or transformations on validated data.
- `common`: (Utility module) Includes models and helpers for running pipelines.

### Execution Order

To ensure consistent and correct results, run the modules in the following order:

1. Ingestion
2. Validation
3. Modeling

Below you'll find the main instructions to run the project.  
For more detailed guidance and module-specific usage, please refer to the `README.md` file inside each module directory.

### Tech Stack

- Kotlin with Spark 3.5
- Delta Lake
- Built using Gradle

---

## Usage Instructions

To run the project and generate the analytical views, follow these steps:

1. Build the project  
   From the root directory, run:

   ``` ./gradlew build ```

2. Run the Ingestion Job  
   This will generate the data files required for validation/modelling. That also generates a file **report/ingestion_report.txt**:

   ``` ./gradlew :ingestion:run```

3. Run the Validation Job  
   This checks data integrity across tables (e.g., foreign keys, duplicates) and writes a file **report/validation_report.txt**:

   ``` ./gradlew :validation:run```

4. Run the Modelling Job: ⚠️ last view with region task was not performed. ⚠️
This step fixes data inconsistencies and creates analytical views and writes a file **report/modelling_report.txt**:

   ``` ./gradlew :modelling:run```

## Bonus Note

This project was proudly built with the help of ChatGPT, which assisted with:

- Generating documentation and code comments 🚀
- Creating test datasets, log file and write 🎩
- Creating file reports, one of few things chatGPT did well 🤯
- Keeping me sane and driving me crazy at the same time during debugging 🧠💥
- Writing readme 💾

👤 Carolina Abs  
🔗 [LinkedIn](https://www.linkedin.com/in/carolinaabs)

# TODOs
- [ ] Missing views
- [ ] Store rejected/ambiguous data separately for auditing
- [ ] Add region using latitude/longitude in a python module with gadm and geopandas

# 🔧 Improvements Overall
- [ ] Enable data versioning and historical tracking -> DynamoDB
- [ ] Add audit logging for job runs and data writes -> DynamoDB
- [ ] Data quality monitoring -> it can be done by notebooks in a repo
- [ ] Add unit tests for individual transformers, not all of them. Better tests ofc.
- [ ] Extract job configurations to a centralized file -> nothing is hardcoded, including column names
- [ ] Polish code style and structure with `ktlint`
- [ ] Obviously read tables rather than files, not possible in my current configuration
- [ ] Spark + Delta Lake Schema Evolution Support
- [ ] Enable Delta Table Versioning and Time Travel
- [ ] Add Delta Table Constraints -> ex: team_id IS NOT NULL
- [ ] Data Lineage Tracking?

# Aim of this coding task

The aim of this coding task is to assess the way you approach problems and design solutions, as well as providing insight into your coding style, expertise and willingness to experiment. It will also provide us with a common ground for a technical interview.

We'd love to see what kind of solution you come up with to the task below and how you approach problem solving.

There is no hard time limit set for this task, but we recommend allocating up to 3 hours to complete this task. Due to time constraints, we don't expect a perfect solution with all the edge cases covered. You’re encouraged to focus on your core strengths and things that you think are important — feel free to leave notes and TODOs if there are parts of implementation you didn’t manage to complete.

# Task Description

## Scenario

Spond provides a platform for organizing sports teams, events, and communication. In this challenge, you’ll simulate ingesting and transforming data about members, teams, and events (including RSVPs) to produce analytics. The focus is on:

1. **Data ingestion** \- reading source files or streaming data.
2. **Data modeling** \- designing efficient, scalable tables/structures.
3. **Transformations & analytics** \- consider how well your solution support queries and pipelines to answer common business questions.
4. **Performance & cost** \- consider impact and improvements to performance and cost in a production setting.
5. **Coding style & solution design** \- showcasing how you structure projects, write clean code, and handle error cases or edge conditions.

## Data Description

You are provided with three sample datasets (in CSV).

| Table | Column | Description |
| :---- | :---- | :---- |
| teams | team\_id (string) | Unique ID |
|  | team\_activity (string) | Activity type of team e.g., football, cricket, rugby, etc. |
|  | country\_code (string) | Alpha-3 country code of group e.g., NOR=Norway; GBR=United Kingdom; etc. |
|  | created\_at (UTC timestamp) | System generated creation timestamp |
| memberships | membership\_id | Unique ID |
|  | team\_id | Foreign Key |
|  | role\_title (string) | member or admin |
|  | joined\_at (UTC timestamp) | System generated creation timestamp |
| events | event\_id | Unique ID |
|  | team\_id | Foreign Key |
|  | event\_start (UTC timestamp) | User-defined event start timestamp |
|  | event\_end (UTC timestamp) | User-defined event end timestamp |
|  | latitude (float) | latitude of event location |
|  | longitude (float) | longitude of event location |
|  | created\_at (UTC timestamp) | System generated creation timestamp |
| event\_rsvps | event\_rsvp\_id | Unique ID |
|  | event\_id | Foreign Key |
|  | member\_id | Foreign Key |
|  | rsvp\_status | Enum (0=unanswered; 1=accepted; 2=declined) |
|  | responded\_at (UTC timestamp) | System generated creation timestamp |

# Requirements

1. ## Data Ingestion

Provide a way to ingest the above data into your chosen data store (assume that the data needs to be extracted from a PostgreSQL database hosted in AWS).

* Show how you handle foreign keys, data types, timestamps, and any malformed data.
* Provide clear setup instructions or scripts so others can replicate your ingestion process and verify that the data has been successfully loaded.
* You do not need to deploy a real cloud service; a local simulation is fine. Show us your approach.

2. ## Data Modelling

Create a data model (tables, views, or equivalent) that will support common analytical patterns. Your model should easily support use-cases listed below. You are not expected to perform these queries, but rather describe how your design is enabling these analyses.

**Analytics Requirements**

* **Daily active teams:** How many distinct teams hosted or updated events each day?
* **RSVP summary:** For each event, indicate how many members responded as accepted, how many responded as declined, and how many did not respond at any given day.
* **Attendance rate:** Over the last 30 days, what’s the average percentage of “Accepted” RSVPs compared to total invites sent?
* **New vs. returning members:** How many new members joined each week, and how many were returning (already joined in a previous week)?
* **Events hosted per region:** How many events were hosted per region (Fylke for Norway, State for the U.S., etc.)?

## Final result should consist of:

* Source code with instructions on how to run it in a Git repository we can access (Github, Bitbucket etc.).
* Extra points for highlighting any identifiable data quality issues, and potential solutions.
* Extra points for test coverage.
* We encourage you to add a description of improvements to your solution that you think would be natural next steps.

#