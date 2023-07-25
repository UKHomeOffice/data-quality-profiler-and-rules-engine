# Configure and Create Assertion Rules to run in the Data Profiler

Create some assertion rules to run over the data source, and some to run over
specific fields. Visualise the pass and failure rates in a data quality report.

As well as the built-in rules, we can create our own custom assertion rules.

There are multiple ways to add new assertion rules. You can either write
a simple one-field named rule using the BuiltIn package, or you can
write a more complex rule that can read whole records, or even whole datasets. 

## Add a single-field "named" rule using the BuiltIn package

A new rule can be added by creating an object in the package:

`uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin`

The object must have a method called `apply` which returns a type of `BuiltIn`.

There are many examples in the Data Profiler source code to see how the `BuiltIn` trait
works, and it must implement the following:

```
trait BuiltIn {
  def name: String
  def rule: RecordValue => Boolean

  def mapPair: (String, RecordValue => Boolean) = name -> rule
}
```

The `name` field is used to invoke the function using the BuiltInFunction call,
which is used in ProfilerConfiguration like so, and the best practice is
to use a camel case name:

```
FieldBasedMask(BuiltInFunction(name = "MaxLength3"))
```

The `rule` field is a function that takes a RecordValue and returns a boolean
to indicate if the field passed the rule or not.

// TODO finish this writeup