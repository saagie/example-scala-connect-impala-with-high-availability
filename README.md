Connect to Impala with High availability
=================

An example to show how to connect to Impala with a random active datanode.


## Usage in Saagie

- sbt assembly
- Create a new Java/Scala Job
- Upload the jar (target/scala-2.11/Impala High Availability-assembly-0.1.jar)
- Add impala user, impala password and the database name in arguments in the command line (The user have to had ALL privilege in the database)
- Use default parameters 
- Create and Launch