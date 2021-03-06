package geotrellis.spark.io.accumulo


import geotrellis.spark.{TileId, TmsTile}
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.accumulo.core.client.mapreduce.lib.util.{ConfiguratorBase => CB}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext


case class AccumuloInstance(
  instanceName: String, zookeeper: String,
  user: String, token: AuthenticationToken)
{
  val instance: Instance = instanceName match {
    case "fake" => new MockInstance("fake") //in-memory only
    case _      => new ZooKeeperInstance(instanceName, zookeeper)
  }
  val connector = instance.getConnector(user, token)

  //TODO: read the table from the configuration
  val metaDataCatalog = new MetaDataCatalog(connector, "metadata")

  def tileCatalog(implicit sc: SparkContext) =
    new AccumuloCatalog(sc, this, metaDataCatalog)


  def setAccumuloConfig(conf: Configuration): Unit = {
    if (instanceName == "fake")
      CB.setMockInstance(classOf[AccumuloInputFormat], conf, instanceName)
    else
      CB.setZooKeeperInstance(classOf[AccumuloInputFormat],conf, instanceName, zookeeper)
    CB.setConnectorInfo(classOf[AccumuloInputFormat], conf, user, token)

  }

  def setAccumuloConfig(job: Job): Unit =
    setAccumuloConfig(job.getConfiguration)

  def setAccumuloConfig(sc: SparkContext): Unit =
    setAccumuloConfig(sc.hadoopConfiguration)
}