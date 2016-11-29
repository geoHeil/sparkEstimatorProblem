# problem with spark estimator

I have a transformer and an estiamtor. The first one works just fine, the second one has no access to the parameter and is using a default value.
This behaviour is wrong / it should have access to the parameter value.

Version of Spark is 2.0.2.

**To reproduce the problem**
  - execute `sbt run`

The result of the transformer ==> it works just fine.

```
+----------+---+--------+
|     dates|ISO|isInList|
+----------+---+--------+
|2016-01-01|ABC|       1|
|2016-01-02|ABC|       1|
|2016-01-03|POL|       0|
|2016-01-04|ABC|       1|
|2016-01-05|POL|       0|
|2016-01-06|ABC|       1|
|2016-01-07|POL|       0|
|2016-01-08|ABC|       1|
|2016-01-09|def|       1|
|2016-01-10|ABC|       1|
+----------+---+--------+

+--------+                                                                      
|isInList|
+--------+
|       1|
|       0|
+--------+
```

The result of the estimator: only 0 ==> default fallback is used.
```
+----------+---+--------+
|     dates|ISO|isInList|
+----------+---+--------+
|2016-01-01|ABC|       0|
|2016-01-02|ABC|       0|
|2016-01-03|POL|       0|
|2016-01-04|ABC|       0|
|2016-01-05|POL|       0|
|2016-01-06|ABC|       0|
|2016-01-07|POL|       0|
|2016-01-08|ABC|       0|
|2016-01-09|def|       0|
|2016-01-10|ABC|       0|
+----------+---+--------+

+--------+
|isInList|
+--------+
|       0|
+--------+

```
Why does the Estimator not pick up the parameter set via `new ExampleEstimator().setIsInList(Array("def", "ABC")).fit(dates).transform(dates)`
