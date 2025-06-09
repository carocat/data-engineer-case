# Common Module

This is a **shared library module** (non-executable) that provides reusable components and definitions used across all other modules in the data platform.

It is not a standalone application and does not run independently. Instead, it is compiled alongside the project and used as a core dependency in modules like `ingestion`, `validation`, and `modeling`.
## Key Purpose

- 🧱 **Centralized logic and schemas**
    - Establishes a modular structure that enforces shared definitions across ingestion, validation, modeling, and any future services
    - Encourages **transparency between teams**: any schema or transformation change becomes immediately visible to all consuming modules

## Key Features

- 📦 **Typed dataset models**
    - Located under the `models` package
    - Define schemas using Kotlin data classes
    - Ensure type safety and consistency throughout the Spark pipelines
    - Example: if the type of a column changes, it triggers compile-time errors in all affected modules, making schema evolution transparent and controlled

- 🛠️ **Shared transformers**
    - Reusable Spark transformations are defined under the `transformers` package
    - Enable common logic (e.g., timestamp parsing, normalization, null handling) to be reused across multiple services
    - Promotes **a single source of truth** for critical transformations, such as financial calculations or data standardization

- 🧪 **BaseTransformer class**
    - Built by extending `org.apache.spark.ml.Transformer`
    - Serves as the abstract base class for all custom transformation logic
    - Enables integration into Spark ML pipelines while maintaining clarity and flexibility
    - Using this design:
        - You can build modular, testable, and reusable transformations
        - Functions remain decoupled from SQL or hardcoded logic
        - Pipelines can be composed using a declarative and structured approach

- ⚙️ **SparkSessionFactory**
    - Provides a unified way to create or reuse a `SparkSession`
    - Helps enforce common configurations and simplifies testability across modules

## Benefits

- ✅ **Schema reinforcement**
    - Centralizing models means breaking schema changes become immediately visible to all developers
    - No need for Slack messages or email chains — breaking changes are caught during development

- ✅ **Improved collaboration**
    - Encourages a shared understanding of data structures
    - Fosters better collaboration across engineering, analytics, and data science teams

- ✅ **Code reuse and maintainability**
    - One logic, one place — avoids duplication and reduces the risk of inconsistencies
    - Improves testability and simplifies debugging across projects

---

This module acts as the backbone of the entire project, and is crucial for ensuring long-term scalability, transparency, and stability of the data platform.
