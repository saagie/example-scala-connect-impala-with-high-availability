Connect to Impala with High availability
=================

An example to show how to connect to Impala with a random active datanode.

Tutorial: https://saagie.zendesk.com/hc/en-us/articles/360011528840-Impala-with-high-availability

## Usage in another SBT project

- Add the folowing code in your `build.sbt` file of your sbt project
```
lazy val gitRepo = "git:https://github.com/saagie/example-scala-connect-impala-with-high-availability.git"
lazy val g = RootProject(uri(gitRepo))
lazy val root = project in file(".") dependsOn g

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
```

- Check if you have already a `plugin.sbt` file in `project/plugins.sbt` in order to use sbt assembly
- Then, in your class, you can use the function `ImpalaHA.get_all_active_datanode` in order to get a list of available datanodes, with the following parameter
    - list_datanodes: `Seq[String]` List of Impala Datanodesn eg: `Seq("dn1","dn2","dn3","dn4")` :rotating_light: in this example, we have four datanodes, you have to change it according to your situation.
    - user: `String` User that we can use to connect to the database 
    - pwd: `String` Password of the user 
    - databaseName: `String` Name of database that user can access eg: `default`
- Or, you can use the function `ImpalaHA.random_node_connect` in order to set-up a connection to a random available datanode with the same parameters.

## Example of use 
```
// Suppose that we have 9 datanodes
val dn =  sys.env.getOrElse("LIST_DATANODES", "dn1;dn2;dn3;dn4;dn5;dn6;dn7;dn8;dn9" )
val dn_list = dn.split(";")
val user = sys.get("MY_USER")
val pwd = sys.get("MY_PWD")
val databaseName = "default"

// Show all available datanodes
val url = get_all_active_datanode(dn_list,user, pwd, databaseName)
logger.info("URL available: ")
url.foreach(logger.info)

// Set-up a connection to a random active datanode 
val con = random_node_connect(dn_list,user, pwd, databaseName)
val stmt = con.createStatement()

// Execute a show tables query and show the result
val sqlStatementShow = "SHOW TABLES"
val resultSet = stmt.executeQuery(sqlStatementShow)
while ( resultSet.next() ) {
  val tableName = resultSet.getString("name")
  logger.info("Table name: " + tableName)
}

// Clean up
stmt.close()
con.close()

```

### Environment variables used
- `PORT_IMPALA`: default value set as: `21050`
- `LIST_DATANODES`
