package geotrellis.spark.io.accumulo

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.utils._
import geotrellis.raster._
import geotrellis.spark.io.hadoop.HdfsUtils

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.accumulo.core.data.{Key, Mutation, Value, Range => ARange}
import org.apache.accumulo.core.client.mapreduce.{AccumuloFileOutputFormat, AccumuloOutputFormat}
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.conf.{AccumuloConfiguration, Property}

import scala.collection.JavaConversions._
import scala.collection.mutable

import spire.syntax.cfor._


trait RasterRDDWriter[K] {

  def getSplits(
    layerId: LayerId,
    metaData: RasterMetaData,
    keyBounds: KeyBounds[K],
    kIndex: KeyIndex[K],
    num: Int = 48
  ): List[String]

  def encode(
    layerId: LayerId,
    raster: RasterRDD[K],
    kIndex: KeyIndex[K]
  ): RDD[(Text, Mutation)]

  def accumuloIngestDir: Path = {
    val conf = AccumuloConfiguration.getSiteConfiguration
    new Path(conf.get(Property.INSTANCE_DFS_DIR), "ingest")
  }

  def write(
    instance: AccumuloInstance,
    layerMetaData: AccumuloLayerMetaData,
    keyBounds: KeyBounds[K],
    kIndex: KeyIndex[K]
  )(layerId: LayerId, raster: RasterRDD[K])(implicit sc: SparkContext): Unit = {
    // Create table if it doesn't exist.
    val tileTable = layerMetaData.tileTable
    if (!instance.connector.tableOperations().exists(tileTable))
      instance.connector.tableOperations().create(tileTable)

    val ops = instance.connector.tableOperations()
    val groups = ops.getLocalityGroups(tileTable)
    val newGroup: java.util.Set[Text] = Set(new Text(layerId.name))
    ops.setLocalityGroups(tileTable, groups.updated(tileTable, newGroup))

    val splits = getSplits(layerId, layerMetaData.rasterMetaData, keyBounds, kIndex, 7)

    instance.connector.tableOperations().addSplits(tileTable, new java.util.TreeSet(splits.map(new Text(_))))

    val job = Job.getInstance(sc.hadoopConfiguration)
    val conf = job.getConfiguration

    val outPath = HdfsUtils.tmpPath(accumuloIngestDir, s"${layerId.name}-${layerId.zoom}", conf)
    val failuresPath = outPath.suffix("-failures")

    try {
      // this directory needs to not exist, will be created by AFOF
      //HdfsUtils.ensurePathExists(outPath, conf)
      HdfsUtils.ensurePathExists(failuresPath, conf)

      encode(layerId, raster, kIndex)
        .saveAsNewAPIHadoopFile(instance.instanceName, classOf[Key], classOf[Value], classOf[AccumuloFileOutputFormat], job.getConfiguration)

      ops.importDirectory(tileTable, outPath.toString, failuresPath.toString, true)
    }
  }
}
