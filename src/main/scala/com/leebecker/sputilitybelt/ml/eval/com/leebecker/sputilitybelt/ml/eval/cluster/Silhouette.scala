package com.leebecker.sputilitybelt.ml.eval.com.leebecker.sputilitybelt.ml.eval.cluster

import java.util.UUID

import com.leebecker.sputilitybelt.util.AddUniqueId
import org.apache.spark.mllib.linalg.{Vector => mlVector, Vectors => mlVectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class SilhouetteMetrics(clusterCol: String, silhouetteCol: String, pointMetrics: DataFrame, clusterMetrics: DataFrame) {

  /**
    * Optionally takes a sample rate to produce the dataset ordered by cluster and silhouette value
    * This makes for easier plotting of the per cluster silhouette distributions as seen in
    * [[http://scikit-learn.org/stable/auto_examples/cluster/plot_kmeans_silhouette_analysis.html Scikit-Learn's silhouette analysis example]]
    * @param sampleRate
    * @return
    */
  def prepareForViz(sampleRate: Option[Double]=None) = {
    // For visualization
    val windowSpec = Window.orderBy(clusterCol, silhouetteCol)
    val getIdx = row_number.over(windowSpec)
    if (sampleRate.isEmpty)
      pointMetrics.withColumn("idx", getIdx)
    else
      pointMetrics.sample(true, sampleRate.get).withColumn("idx", getIdx)
  }

  /**
    * Averages the silhouette score across all clusters
    */
  lazy val silhouetteScore = clusterMetrics.select(mean(col(silhouetteCol)))

}


/**
  * Utility class for computing silhouette metrics for a clustered dataset.  For details about the math and methodology
  * refer to the [[https://en.wikipedia.org/wiki/Silhouette_(clustering) Wikipedia entry on silhouette analysis]]
  * @param clusterCol name of column containing assigned clusters (default=cluster)
  * @param featuresCol name of column containing feature vectors used for clustering (default=features)
  * @param idCol name of column containing (or to contain) a unique ID for the row (default="id")
  * @param generateId  specifies whether or not to assign a unique ID to each row in the data (default=false)
  */
class Silhouette (
    clusterCol: String="cluster",
    featuresCol: String="features",
    idCol: String="id",
    generateId: Boolean=false) {


  val addId = new AddUniqueId().setOutputCol(idCol)

  val vecDist = udf((v1: mlVector, v2: mlVector)=>mlVectors.sqdist(v1, v2))

  val rowMax = udf((x: Double, y: Double) => math.max(x,y))

  val uid = UUID.randomUUID()

  /**
    * Computes silhouette metrics, both per point and per cluster
    * @param df
    * @return
    */
  def compute(df: DataFrame): SilhouetteMetrics = {
    import df.sqlContext.implicits._

    // Add unique ID if necessary
    // IDs are used for producing a cartesian product
    val clustersDf = if (generateId) addId.transform(df) else df

    // Temporary columns used for computing silhouette scores
    val cluster2Col = s"${uid}_features2"
    val features2Col = s"${uid}_cluster2"
    val distanceCol = s"${uid}_distance"
    val dissimilarityCol = s"${uid}_dissimilarity"
    val assignedDissimilarityCol = s"${uid}_assignedDissimilarity"
    val neighborDissimilarityCol = s"${uid}_neighborDissimilarity"

    // Perform inefficient cartesian join to compute pairwise distances between points
    val df1 = clustersDf.select(col(idCol), col(clusterCol), col(featuresCol))
    val df2 = clustersDf.select(col(clusterCol).as(cluster2Col), col(featuresCol).as(features2Col))
    val pointToPointDistances = df1.join(df2).withColumn(distanceCol, vecDist(col(featuresCol), col(features2Col)))

    // Dissimilarity for a point is the mean distance between a point and all points in the cluster (self-included for simplicity)
    val pointToClusterDissimilarities = pointToPointDistances
      .groupBy(col(idCol), col(clusterCol), col(cluster2Col)).agg(mean(col(distanceCol)).as(dissimilarityCol))

    // Retrieve a_i = dissimilarities for clustering algorithm assigned clusters.
    val assignedScore = pointToClusterDissimilarities
      .where(col(clusterCol) === col(cluster2Col))
      .drop(col(cluster2Col)).withColumnRenamed(dissimilarityCol, assignedDissimilarityCol)

    // Compute b_i = minimum dissimilarity for a point outside of its assigned cluster
    val nearestNeighborScore = pointToClusterDissimilarities
      .where(col(clusterCol) !== col(cluster2Col))
      .groupBy(col(idCol))
      .agg(min(col(dissimilarityCol)).as(neighborDissimilarityCol))

    // Compute silhouette score per point -
    val silhouetteCol = "silhouette"
    val pointSilhouetteScores = assignedScore.as("assigned")
      .join(nearestNeighborScore.as("nn"), col(s"assigned.$idCol")===col(s"nn.$idCol")).drop(col(s"assigned.$idCol"))
      .withColumn(silhouetteCol, (col(neighborDissimilarityCol) - col(assignedDissimilarityCol)) / rowMax(col(assignedDissimilarityCol), col(neighborDissimilarityCol)))

    // Compute mean silhouette score per cluster
    val clusterSilhouetteScores  = pointSilhouetteScores.groupBy(clusterCol).agg(mean(col(silhouetteCol)).as(silhouetteCol))

    SilhouetteMetrics(clusterCol, silhouetteCol, pointSilhouetteScores, clusterSilhouetteScores)
  }

}
