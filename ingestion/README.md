# Ingestion Module

This module is responsible for loading and preparing the raw data for downstream processing.  
The source files are CSVs, and custom schemas are used to ensure accurate parsing of column types.

## Key Features

- ✅ Reads CSV files using **explicit schemas**  
  This avoids relying on `inferSchema = true`, ensuring consistent and expected column types across runs.

- 🧱 **Data transformations using Spark functions**  
  Located in the `transformers` package, these include:
    - Null handling (e.g., dropping or replacing nulls)
    - Type casting and normalization
    - Timestamp parsing and validation using `try_to_timestamp`, which allows detection of malformed strings when dates are read as `String`
    - All transformations are implemented using Spark functions, **not hardcoded SQL**, which improves flexibility, reuse, and robustness (avoids typos and hard-to-maintain logic)

- 🗂️ **Partitioned output**  
  Data is located at the root project in a folder named **data**, the datasets are partitioned by `year`, `month`, and `day`.  
  This assumes a daily ingestion process and enables:
    - Efficient reads during processing (only scan relevant partitions)
    - Better performance for downstream queries and modeling
    - Scalability over time as the data volume grows

- ⚙️ **Pipeline orchestration**  
  Found in the `pipelines` package, these orchestrate:
    - Reading input data
    - Applying transformations
    - Writing the final outputs (e.g., Parquet or Delta)  
      The orchestration is modular and avoids hardcoded logic, making it easy to maintain or extend.

- 📚 **Typed dataset models**  
  Defined in the `models` package using Kotlin data classes:
    - These classes define the schema shared across all modules
    - Enable compile-time checks and Dataset-based transformations
    - Help enforce consistency, making the project modular and predictable

## Execution

From the root of the project, first build the module using Gradle:

## Execution

From the root of the project, first build the module using Gradle:

``` ./gradlew build ```

Then, to run the ingestion pipeline:

``` ./gradlew :ingestion:run```



This will execute the pipeline and generate the following outputs:

- ✅ A report saved at:  
  `report/ingestion_report.txt`  
  This file contains sample data and metadata produced during ingestion.

- ✅ Delta Lake and parquet folders with the ingested datasets  
- Big data tools plugin in Intellij can be used to view parquet datasets
  These are written to your configured output location, partitioned by `year`, `month`, and `day`.

> Note:  
> Delta **tables** are not created directly due to local environment limitations.  
> However, the code includes logic to create them, and in a production environment, the tables would be properly registered in the metastore.  
> Importantly, **querying Delta files directly (without tables) uses the same APIs as querying Delta tables**, so this approach allows the entire solution to run locally without functional loss.  
> Creating the files ensures that queries and pipelines remain realistic and compatible with production behavior.

---

For more context on how this module fits in the larger project, please check the root `README.md`.
