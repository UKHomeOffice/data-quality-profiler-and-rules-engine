# Data Quality Profiler and Rules Engine

Provides the following:

* Data Profilers for large volume data profiling in Spark
* Assertion rule definitions and checking
* Reference data loading and joining
* Excel and CSV reference data parsing
* JSON output enriched with data quality markers/profilers
* Metrics and summary dataframe output
* Dimensional tagging of profiler outputs (additional identifiers)
* JSON flattener
* JSON and CSV loader, extensible to other formats
* Custom key pre-processor and custom parquet row reader functionality
* Comprehensive built-in assertion rules modules, extensible
* Built-in set of field-level profile masks
* Compound assertion rule definition (i.e. a set of sub-rules must all pass)
* Human-readable Data Quality and Assertion Rule Compliance report output


# Repository Layout

* Data Quality Profiler and Rules Engine code: [data-quality-profiler](data-quality-profiler)
* Examples and Usage: [examples](examples)


## Usage

Releases are being managed by `6point6` at: https://github.com/6point6/data-quality-profiler-and-rules-engine

Changes are pushed upstream to the `UKHomeOffice` repo at: https://github.com/UKHomeOffice/data-quality-profiler-and-rules-engine


To use the Data Profiler classes, add the following dependency to your `build.sbt`, where the library is published to Maven Central:

    libraryDependencies += "io.github.6point6" %% "data-quality-profiler-and-rules-engine" % "1.1.0"


## Authors

Feel free to contex the authors for help/assistance.

Dr Daniel A. Smith - [dan.smith@6point6.co.uk](mailto:dan.smith@6point6.co.uk) - [@danielsmith-eu](https://github.com/danielsmith-eu)


# Licence

Licensed under the MIT License. See [LICENSE](LICENSE)
