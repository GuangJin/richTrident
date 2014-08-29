#RichTrident
The *RichTrident* project is an extension for [*Apache's Storm*](http://github.com/apache/incubator-storm). Through richTrident, more types of queries can be used against state.

##Background
[Apache's Storm](https://github.com/apache/incubator-storm) provides a distributive framework to process streams. Storm provides an interesting feature, called [Trident](http://storm.incubator.apache.org/documentation/Trident-tutorial.html). Through Trident, any stream have a stateful representation, called  [state](http://storm.incubator.apache.org/documentation/Trident-state). A stream, then, is able to query the state generated from another stream. 

Current Storm implements its state through hashmapping. Thus, current Storm only supports exact key matching, i.e., the equality predicate. *RichTrident*, on the other hand, implements state by a more generic [Map](http://docs.oracle.com/javase/7/docs/api/java/util/Map.html). For example, in RichTrident, [TreeMap](http://docs.oracle.com/javase/7/docs/api/java/util/TreeMap.html) is used to keep the state of any 1D comparable value. In addition, RichTrident also implements a RTreeMap for 2D geo-values. In such a way, RichTrident creates distributed indexed states, based on which more query preidcate logics are supported.

##Prerequisites
All the [prerequisites by the original Storm](http://storm.incubator.apache.org/documentation/Maven.html), such as Java, git and maven, are needed in RichTrident.

##Play with RichTrident
1. To play with richTrident, first clone it by `git clone https://github.com/GuangJin/richTrident.git`.

2. Run `mvn eclipse:eclipse` to create the eclipse project and download dependent jars through maven.

3. Import RichTrident from wherever your clone is, Eclipse shall find two Java projects:
	..*	richTrident-core: where the core functions of richTrident are implemented
	..*	richTrident-examples have three exampls on different types of queries can be used in RichTrident.

##Note	
Only the local mode has been tested so far.
