# Modeling Module

This module performs the final preparation and structuring of data for analytical and business use cases.  
It consumes the cleaned and validated datasets from the ingestion process and produces curated views for downstream consumption.

## Key Features

- 📦 **Reads from previously ingested datasets**
    - Consumes Delta datasets produced by the `ingestion` module
    - Uses **typed Kotlin models** to enforce schema consistency across modules

- 🧹 **Data cleaning**
    - Applies filters to remove records flagged as invalid in the `validation` stage:
        - Memberships without matching Teams
        - Events without valid Memberships
        - Event RSVPs with no corresponding Event or Membership
    - This ensures that only referentially correct records are retained in the modeled views

- 🧼 **Anomaly handling (conceptual)**
    - Although not implemented due to time constraints, the architecture assumes:
        - Invalid records should be moved to a **quarantine** folder
        - This folder would serve for debugging, auditing, and data recovery
        - No data should be silently dropped — all removals should be traceable
    - This kind of flow would ideally be handled during ingestion in a real-world scenario

- 👁️ **View generation**
    - Curated views are created from the cleaned datasets to represent business-ready information
    - These views include:
        - `active_members_per_team_view`
        - `rsvp_rate_per_event_view`
        - `rsvp_rate_per_team_view`
    - Each view corresponds to a specific analytical query defined in the `queries` package


## Execution

If the project has already been built (e.g., with `./gradlew build`), you can run this module directly:

```./gradlew :modeling:run```


This will execute the modeling pipeline and generate:

- ✅ Views stored in-memory (or accessible via Spark SQL) under the specified names
- ✅ Cleaned, integrity-checked datasets used as input to the views

> Notes:  
> This module enforces schema consistency through typed dataset models to ensure safety across module boundaries.  
> While data integrity is guaranteed through validation filtering, in a production-grade system, **quarantining of invalid records** would also be implemented to prevent data loss and enable traceability.  
> Views are derived using Spark transformations, ensuring that the logic remains flexible, testable, and maintainable.

---

For more context on how this module fits in the larger project, please check the root `README.md`.