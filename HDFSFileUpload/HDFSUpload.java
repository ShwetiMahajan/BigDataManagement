package edu.ucr.cs.cs226.smaha004;
/**
 * Created by shwetimahajan on 1/23/18.
 */
//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//


import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSUpload {
    public HDFSUpload() {
    }

    public static void main(String[] args) throws Exception {
        long start_time = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
        if(args.length < 2) {
            System.err.println("ERROR : Please enter valid syntax - edu.ucr.cs.cs226.smaha004.HDFSUpload [local_input_path] [hdfs_output_path]");
            System.exit(1);
        }
        String local_input_path = args[0];
        if(!(new File(local_input_path)).exists()) {
            System.err.println("ERROR : The Input path is invalid.");
            System.exit(1);
        }

        FileInputStream fis = new FileInputStream(local_input_path);
        InputStream ins = new BufferedInputStream(fis);
        String destination_file_path = args[1];
        Configuration config = new Configuration();
        System.out.println("Connecting to -- " + config.get("fs.defaultFS"));
        FileSystem fs = FileSystem.get(config);
        Path output_path = new Path(destination_file_path);
        if(fs.exists(output_path)) {
            System.err.println("ERROR : The output path already exists");
            System.exit(1);
        }

        OutputStream outs = fs.create(output_path);
        IOUtils.copyBytes(ins, outs, config);
        System.out.println(destination_file_path + " copied to HDFS");
        long end_time = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
        long total_time = end_time-start_time;
        System.out.println("Total time taken(in seconds) : " + total_time);
    }
}
