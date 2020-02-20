package io.saagie.impalaHA

import java.sql.{Connection, DriverManager}
import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Failure, Random, Success, Try}

object ImpalaHA {
  val logger: Logger = LoggerFactory.getLogger(getClass)


  /**
   * Return a list of available dn
   * @param list_datanodes List of Impala Datanodes
   * @param user          user that we can use to connect to the database
   * @param pwd           password of the user
   * @param databaseName  Name of database that user can access
   */
  def get_all_active_datanode(list_datanodes: Seq[String], user: String, pwd: String, databaseName: String): Seq[String] = {
    val JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(JDBC_DRIVER_NAME)
    list_datanodes.flatMap(dn => connection_dn(dn, user, pwd, databaseName) match {
      case Success(value) => Some(dn)
      case Failure(e) => None
    })
  }

  /**
   * Connect to Impala with an active random DataNode
   * @param list_datanodes List of Impala URLs
   * @param user          username that has the rights to write into /user/hive/warehouse/
   * @param pwd           password of the user
   * @param databaseName  Name of database that user can access
   */
  def random_node_connect(list_datanodes: Seq[String], user: String, pwd: String, databaseName: String):Connection ={
    val data_node_list = list_datanodes.toBuffer
    while (data_node_list.nonEmpty){
      val r = Random.nextInt(data_node_list.size)
      connection_dn(list_datanodes(r), user, pwd, databaseName) match {
        case Success(con) => return con
        case Failure(e) => data_node_list -= list_datanodes(r)
      }
    }
    throw new Exception("No DataNode available")
  }

  /**
   * Try to connect to a specific datanode
   * @param datanode_uri A datanode eg: dn1
   * @param user          username that has the rights to write into /user/hive/warehouse/
   * @param pwd           password of the user
   * @param databaseName  Name of database that user can access
   */

  def connection_dn(datanode_uri: String, user:String, pwd:String, databaseName: String = "default"):Try[Connection] = {
    val JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(JDBC_DRIVER_NAME)
    val connectionURL = "jdbc:hive2://" + datanode_uri + ":" + sys.env.getOrElse("PORT_IMPALA", "21050") + "/" + databaseName
    Try(DriverManager.getConnection(connectionURL, user, pwd))
  }


}
