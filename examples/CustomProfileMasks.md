# Configure Custom Profile Masks

The full source of this example can be found here:

[src/main/scala/uk/gov/ipt/das/dataprofiler/example/CustomProfileMasks.scala](src/main/scala/uk/gov/ipt/das/dataprofiler/example/CustomProfileMasks.scala)

## Single-field Profile Masks

These type of masks run on a single value from the source data

Custom profile masks must implement the `MaskProfiler` trait, which is:

```
trait MaskProfiler {
  def profile(value: RecordValue): RecordValue

  def getDefinition: FeatureDefinition

  def getFeatureOutput(value: RecordValue): FeatureOutput = {
    val featureValue = profile(value)
    if (featureValue == null) {
      FeatureOutput(feature = getDefinition, value = NullValue())
    } else {
      FeatureOutput(feature = getDefinition, value = featureValue)
    }
  }

  def asMask: FieldBasedMask =
    FieldBasedMask(this)
}
```

The `profile` function takes a `RecordValue` and returns a `RecordValue` - usually
these are both actually a `StringValue`, or a `NullValue`, but as is always the case
in this domain, the types cannot be assumed.

As this type of profiler is a mask, it works on a single value input.

The `getDefinition` field returns the `FeatureDefinition`, which defines the
name of the mask, which are used as the FeatureName in the output, i.e.
`DQ_HIGHGRAIN` is the field name for a high grain profile mask.

Examples of mask profilers can be seen in `HighGrainProfile` and `LowGrainProfile`.


## Add a new mask to count the numeric characters in a string

In this example we will create a mask that returns a count of the numeric characters in
a string.

For example the input string: `Happy101` has `3` numeric characters (`1`, `0`, and `1`).

Our new profiler class must extend the `MaskProfiler` trait, and implement the logic:

```
  class CountNumericCharactersMask extends MaskProfiler with Serializable {
    override def profile(value: RecordValue): RecordValue =
      LongValue(value.valueAsString match { case null => 0L case s: String => s.count(c => c.isDigit) })
        
    override def getDefinition: FeatureDefinition =
        FeatureDefinition("DQ", "NUMERIC_CHAR_COUNT")
  }
```

Here we provide the logic to count the numeric characters, specifically by calling `count`
on the result of the `Character.isDigit` function. We then wrap the result in a `LongValue`.

The `FeatureDefinition` is simply the short name of the mask used in the output, we have
specified `NUMERIC_CHAR_COUNT` for simplicity.

Note that the class must also be `with Serializable` for Spark to be able to use it.

## Analysis

Profile masks are passed as part of the `ProfilerConfiguration`, in the sample class
[CustomProfileMasks](src/main/scala/uk/gov/ipt/das/dataprofiler/example/CustomProfileMasks.scala)
we output the whole Metrics dataframe, which shows each
`FeaturePath` (the JSON path to the field), and the count of each output of the
profile mask.

For example, for the `authors_parsed[][]` path we can see the result:

```
|authors_parsed[][]|DQ_NUMERIC_CHAR_COUNT|0           |9505 |
|authors_parsed[][]|DQ_NUMERIC_CHAR_COUNT|2           |21   |
|authors_parsed[][]|DQ_NUMERIC_CHAR_COUNT|6           |9    |
|authors_parsed[][]|DQ_NUMERIC_CHAR_COUNT|1           |3    |
|authors_parsed[][]|DQ_NUMERIC_CHAR_COUNT|4           |1    |
```

This means that `9505` of the values had no numeric characters in them, `21` values
had `2` numeric characters, `9` had `6`, `3` had `1`, and `1` had `4`.

Since this are presumably author names, this seems a bit weird - people's names don't
usually have names in them. Lets output some samples to see what is going on:

```
results.take(1).foreach { arXivResults =>
  arXivResults._2.getMetrics().dataFrame.map { metricsDataframe =>
    println("ArXiv First 1000 records, samples from authors_parsed[][]:")
    metricsDataframe
      .where(col(FEATURE_PATH) === "authors_parsed[][]")
      .orderBy(col(FEATURE_PATH).asc, col(COUNT).desc, col(FEATURE_VALUE).asc)
      .show(numRows = 1000, truncate = false)
  }
}
```

We filter the metrics by the feature path `authors_parsed[][]`, and this time we
don't drop the samples, which shows this result:

```
+------------------+---------------------+------------+---------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+-----+
|featurePath       |featureName          |featureValue|sampleMin                                                                                                            |sampleMax                                                                                                            |sampleFirst                                                                                                          |sampleLast                                                                                                           |count|
+------------------+---------------------+------------+---------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+-----+
|authors_parsed[][]|DQ_NUMERIC_CHAR_COUNT|0           |                                                                                                                     |Šrámek                                                                                                               |Balázs                                                                                                               |                                                                                                                     |9505 |
|authors_parsed[][]|DQ_NUMERIC_CHAR_COUNT|2           |1 and
  7                                                                                                            |2 and 8                                                                                                              |1 and 2                                                                                                              |1 and 8                                                                                                              |21   |
|authors_parsed[][]|DQ_NUMERIC_CHAR_COUNT|6           |1 - Swarthmore College; 2 - Vanderbilt; 3 - Caltech; 4
  - Wesleyan University; 5 - SUNY Stony Brook; 6 - UC Berkeley|1 - Swarthmore College; 2 - Vanderbilt; 3 - Caltech; 4
  - Wesleyan University; 5 - SUNY Stony Brook; 6 - UC Berkeley|1 - Swarthmore College; 2 - Vanderbilt; 3 - Caltech; 4
  - Wesleyan University; 5 - SUNY Stony Brook; 6 - UC Berkeley|1 - Swarthmore College; 2 - Vanderbilt; 3 - Caltech; 4
  - Wesleyan University; 5 - SUNY Stony Brook; 6 - UC Berkeley|9    |
|authors_parsed[][]|DQ_NUMERIC_CHAR_COUNT|1           |I3M                                                                                                                  |the Carinae D-1                                                                                                      |LUTH, CNRS / Observatoire de Paris / Univ. Paris 7                                                                   |I3M                                                                                                                  |3    |
|authors_parsed[][]|DQ_NUMERIC_CHAR_COUNT|4           |the ALADiN2000                                                                                                       |the ALADiN2000                                                                                                       |the ALADiN2000                                                                                                       |the ALADiN2000                                                                                                       |1    |
+------------------+---------------------+------------+---------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------+-----+
```

Where the `featureValue` is `0` (i.e., where there are no numeric characters),
the samples are all human names, which is what we expect.

Where the feature value is `2`, (i.e., 2 numeric characters), the samples are
like `2 and 8` - this indicates a data quality issue.

Similarly for the other values, there are multi-line lists of institute names,
project names, etc. which are not human names and could indicate a data quality
issue.







