# parma22

the sample command to run is:
reset && HADOOP_CLASSPATH="$HADOOP_CLASSPATH:/opt/MyJarTests/commons-math3-3.6.1.jar" hadoop jar parma2.jar parmanix.MapReduceEntrypoint /input_bigshop/bigshop.txt /parmanix_output500 bigshop parmanix.IdentityMapper count 1.0 1 0 50000 10000 15 900000
