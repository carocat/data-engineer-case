# Validation Module

This module is responsible for checking data consistency and integrity between related datasets.  
It performs checks for nulls in unique identifiers and verifies referential integrity between parent and child datasets.

## Key Features

- 🔍 **Primary and foreign key validation**
    - Ensures that key fields such as IDs (used as primary keys) are not null
    - Detects inconsistencies in referential relationships between datasets

- 🧬 **Hierarchical integrity checks**
    - The validation logic follows a top-down approach:
      ```
      Team
        └── Membership
              └── Event
                    └── EventRsvp
                          ↳ (also references Membership)
      ```
    - Each dataset is validated to ensure its foreign key references exist in the parent dataset:
        - Memberships without matching Teams
        - Events without valid Memberships
        - Event RSVPs without corresponding Events or Memberships

- ✅ Guarantees that downstream processing is built on clean, referentially correct data

## Execution

If you've already built the project (e.g., with `./gradlew build`), you can run this module directly:

```./gradlew :validation:run ```



This will execute the validation pipeline and generate the following output:

- ✅ A validation report saved at:  
  `report/validation_report.txt`  
  This file contains the results of null checks and foreign key integrity validations.

> Notes:  
> These checks are implemented using Spark transformations, not hardcoded SQL, which ensures flexibility, reusability, and better maintainability.  
> For performance optimization, **broadcast joins** are used when joining with root tables like `Team`.  
> Since these root datasets are expected to be relatively small compared to downstream tables, broadcasting them avoids expensive data shuffling during join operations and significantly improves execution speed.

---

For more context on how this module fits in the larger project, please check the root `README.md`.
