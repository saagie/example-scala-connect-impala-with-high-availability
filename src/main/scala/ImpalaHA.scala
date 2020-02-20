import java.sql.{Connection, DriverManager}

import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Random, Success, Try}
import scala.collection.mutable.ListBuffer

object ImpalaHA {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]):Unit = {
    val dns =  sys.env.getOrElse("LIST_DATANODES", "dn1;dn2;dn3;dn4;dn5;dn6;dn7;dn8;dn9" )
    val dn_list = dns.split(";")
    if(args.length < 3){
      logger.warn("Three arg are required:\n \t- user, pwd, database ")
    }
    val user = args(0)
    val pwd = args(1)
    val databaseName = args(2)

    val url = get_all_active_datanode(dn_list,user, pwd, databaseName)
    logger.info("URL available: ")
    url.foreach(logger.info)
    val con = random_node_connect(dn_list,user, pwd, databaseName)
    val stmt = con.createStatement()
    val sqlStatementShow = "SHOW TABLES"
    val resultSet = stmt.executeQuery(sqlStatementShow)
    while ( resultSet.next() ) {
      val tableName = resultSet.getString("name")
      logger.info("Table name: " + tableName)
    }
    stmt.close()
    con.close()

  }

  /**
   * Return a list of available dn
   * @param list_datanodes List of Impala Datanodes
   * @param user          user that we can use to connect to the database
   * @param pwd           password of the user
   * @param databaseName  Name of database that user can access
   */
  def get_all_active_datanode(list_datanodes: Seq[String], user: String, pwd: String, databaseName: String): Seq[String] = {
    val JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver"
    val list_active_name_node = new ListBuffer[String]()
    Class.forName(JDBC_DRIVER_NAME)
    var cpt = 1
    for (dn <- list_datanodes) {
      val connectionURL = "jdbc:hive2://" + dn + ":" + sys.env.getOrElse("PORT_IMPALA", "21050") + "/"+databaseName
      try {
        // Test if the connection is working
        val con = DriverManager.getConnection(connectionURL, user, pwd)
        list_active_name_node += dn
        con.close()
      }
      catch {
        case e: Throwable =>
          if (cpt == list_datanodes.size) {
            // if we tested both NameNodes, it means there is no active NameNode
            throw new Exception("No DataNode available")
          }
          else {
            cpt += 1

          }
      }
    }
    list_active_name_node.toList
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
