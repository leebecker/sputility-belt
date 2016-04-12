package com.leebecker.sputilitybelt.ml.eval

import org.apache.spark.annotation.{DeveloperApi, Since}

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vector => mlVector, Vectors => mlVectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class SilhouetteMetrics(clusterCol: String, silhouetteCol: String, pointMetrics: DataFrame, clusterMetrics: DataFrame) {

  /**
    * Optionally takes a sample rate to produce the dataset ordered by cluster and silhouette value
    * This makes for easier plotting of the per cluster silhouette distributions as seen in
    * [[http://scikit-learn.org/stable/auto_examples/cluster/plot_kmeans_silhouette_analysis.html Scikit-Learn's silhouette analysis example]]
    *
    * @param fraction - number from 0.0 to 1.0 indicating what percent of the data to sample
    * @return
    */
  def prepareForViz(fraction: Option[Double]=None) = {
    // For visualization
    val windowSpec = Window.orderBy(clusterCol, silhouetteCol)
    val getIdx = row_number.over(windowSpec)
    if (fraction.isEmpty)
      pointMetrics.withColumn("idx", getIdx)
    else
      pointMetrics.sample(withReplacement=true, fraction = fraction.get).withColumn("idx", getIdx)
  }

  /**
    * Averages the silhouette score across all clusters
    */
  lazy val silhouetteScore = clusterMetrics.select(mean(col(silhouetteCol)))

}


/**
  * Utility class for computing silhouette metrics for a clustered dataset.  For details about the math and methodology
  * refer to the [[https://en.wikipedia.org/wiki/Silhouette_(clustering) Wikipedia entry on silhouette analysis]]
  */
class Silhouette(override val uid: String) extends Evaluator {

  def this() = this(Identifiable.randomUID("silhouetteEval"))

  val clusterCol: Param[String] = new Param(this, "clusterCol", "cluster label column name")

  def getClusterCol: String = $(clusterCol)

  def setClusterCol(value: String): this.type = set(clusterCol, value)

  val featuresCol: Param[String] = new Param(this, "featuresCol", "feature vectors column name")

  def getFeaturesCol: String = $(featuresCol)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  val idCol: Param[String] = new Param(this, "idCol", "column name containing unique id")

  def getIdCol: String = $(idCol)

  def setIdCol(value: String): this.type = set(idCol, value)




  val generatedIds: BooleanParam = new BooleanParam(this, "generatedIds", "if true, will generate IDs for idCol")

  def getGeneratedIds: Boolean = $(generatedIds)

  def setGenerateIds(value: Boolean): this.type = set(generatedIds, value)

  setDefault(
    clusterCol -> "cluster",
    featuresCol -> "features",
    idCol -> "id",
    generatedIds -> false
  )

  val vecDist = udf((v1: mlVector, v2: mlVector) => mlVectors.sqdist(v1, v2))

  val rowMax = udf((x: Double, y: Double) => math.max(x, y))

  /**
    * Computes silhouette metrics, both per point and per cluster
    *
    * @param df - data frame containing cluster labels and feature vectors
    * @return
    */

  /*override*/ def evaluate(df: DataFrame): Double = {
    import df.sqlContext.implicits._

    // Add unique ID if necessary
    // IDs are used for producing a cartesian product
    val clustersDf = if (getGeneratedIds) df.withColumn(getIdCol, monotonicallyIncreasingId) else df

    // Temporary columns used for computing silhouette scores
    val cluster2Col = s"${uid}_features2"
    val features2Col = s"${uid}_cluster2"
    val distanceCol = s"${uid}_distance"
    val dissimilarityCol = s"${uid}_dissimilarity"
    val assignedDissimilarityCol = s"${uid}_assignedDissimilarity"
    val neighborDissimilarityCol = s"${uid}_neighborDissimilarity"

    // Perform inefficient cartesian join to compute pairwise distances between points
    val df1 = clustersDf.select(col(getIdCol), col(getClusterCol), col(getFeaturesCol))
    val df2 = clustersDf.select(col(getClusterCol).as(cluster2Col), col(getFeaturesCol).as(features2Col))
    val pointToPointDistances = df1.join(df2).withColumn(distanceCol, vecDist(col(getFeaturesCol), col(features2Col)))

    // Dissimilarity for a point is the mean distance between a point and all points in the cluster (self-included for simplicity)
    val pointToClusterDissimilarities = pointToPointDistances
      .groupBy(col(getIdCol), col(getClusterCol), col(cluster2Col)).agg(mean(col(distanceCol)).as(dissimilarityCol))

    // Retrieve a_i = dissimilarities for clustering algorithm assigned clusters.
    val assignedScore = pointToClusterDissimilarities
      .where(col(getClusterCol) === col(cluster2Col))
      .drop(col(cluster2Col)).withColumnRenamed(dissimilarityCol, assignedDissimilarityCol)

    // Compute b_i = minimum dissimilarity for a point outside of its assigned cluster
    val nearestNeighborScore = pointToClusterDissimilarities
      .where(col(getClusterCol) !== col(cluster2Col))
      .groupBy(col(getIdCol))
      .agg(min(col(dissimilarityCol)).as(neighborDissimilarityCol))

    // Compute silhouette score per point -
    val silhouetteCol = "silhouette"
    val pointSilhouetteScores = assignedScore.as("assigned")
      .join(nearestNeighborScore.as("nn"), col(s"assigned.$getIdCol") === col(s"nn.$getIdCol")).drop(col(s"assigned.$getIdCol"))
      .withColumn(silhouetteCol, (col(neighborDissimilarityCol) - col(assignedDissimilarityCol)) / rowMax(col(assignedDissimilarityCol), col(neighborDissimilarityCol)))

    // Compute mean silhouette score per cluster
    val clusterSilhouetteScores = pointSilhouetteScores.groupBy(getClusterCol).agg(mean(col(silhouetteCol)).as(silhouetteCol))

    val res =clusterSilhouetteScores.select(mean(silhouetteCol).as(silhouetteCol)).first()
    this.silhouetteMetrics = SilhouetteMetrics(getClusterCol, silhouetteCol, pointSilhouetteScores, clusterSilhouetteScores)

    res.getDouble(0)
  }



  var silhouetteMetrics: SilhouetteMetrics = null

  def copy(extra: ParamMap): Silhouette = defaultCopy(extra)
}

object Silhouette {
  def create(clusterCol: String="cluster", featuresCol: String="features", idCol: String="id", generateIds: Boolean=false) = {
    new Silhouette()
      .setClusterCol(clusterCol)
      .setFeaturesCol(featuresCol)
      .setIdCol(idCol)
      .setGenerateIds(generateIds)
  }
}
