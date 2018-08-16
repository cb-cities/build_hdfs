This transforms a JSON form graph object and writes it to HDFS for it to be used by GraphX.

### Kerberos tgt's

To setup or refresh kerberos credentials:

```kinit -k -t ~/keytab -r 7d `whoami` ```

### Load modules

`module load scala`
`module load sbt`

### Package

`cd build_hdfs`
`sbt package`

### Execution

`spark-submit --class "hdfs_build" --master local[16] --executor-memory 20G target/scala-2.10/hdfs_build_2.10-1.0.jar`