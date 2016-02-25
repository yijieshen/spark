## ColumnVector expression update/Authenticate

Since I've changed the implementation of String ColumnVector from UTF8String[] to (bytesVector[][], starts[] and lengths[]), the expressions related to
String Column Manipulation should be updated.

* [ ] BatchExpression.scala
* [ ] BatchPredicate.scala
* [ ] BatchProjection.scala
* [ ] EquivalentBatchExpressions.scala
* [ ] batchArithmetic.scala
* [ ] batchCast.scala
* [ ] batchConditionalExpressions.scala
* [ ] batchNullExpressions.scala
* [ ] batchPredicates.scala
* [ ] batchStringExpressions.scala
* [ ] leafBatchExpressions.scala
