import java.sql.{Connection, DriverManager}
import org.slf4j.{Logger, LoggerFactory}
import scala.util.Random
import scala.collection.mutable.ListBuffer

object ImpalaHA {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]):Unit = {
    val dns =  sys.env.getOrElse("LIST_DATANODES", "dn1;dn2;dn3;dn4;dn5;dn6;dn7;dn8;dn9" )
    val dn_list = dns.split(";")
    if(args.size < 3){
      logger.warn("3 arg is required:\n \t- user, pwd, database ")
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
    stmt.execute(sqlStatementShow)

  }

  /**
   * Return a list of available dn
   * @param list_datanodes List of Impala URLs
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
    if (list_datanodes.isEmpty)
      throw new Exception("nodelist is mandatory, please provide it.")
    val available_dns = get_all_active_datanode(list_datanodes, user, pwd, databaseName)
    val r = Random.nextInt(available_dns.size)
    val connectionURL = "jdbc:hive2://"+available_dns(r)+":"+sys.env.getOrElse("PORT_IMPALA", "21050")+"/" + databaseName
    DriverManager.getConnection(connectionURL, user, pwd)
  }


}
