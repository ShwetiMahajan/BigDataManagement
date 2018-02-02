To run the file :
1. Build the maven project with the given po.xml and include the HDFSUpload.java file.
2. Optain the jar file produced in the target folder.
3. It will be convenient to save the jar into the hadoop/share folder in the hadoop home directory.
4. Start the hadoop namenode and datanodes(make sure they're both running on the browser : http://localhost:50070/dfshealth.html).
5. Run the command : bin/hadoop jar share/hadoop/my_examples/[Name_of_the_JAR_file].jar HDFSUpload [local_file_path] [HDFS_output_path].
