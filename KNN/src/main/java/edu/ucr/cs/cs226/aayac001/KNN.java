package edu.ucr.cs.cs226.aayac001;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.*;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class KNN {

	 public static class KNNMapper extends Mapper<Object, Text, DoubleWritable, Text>{
	        private String qp_x, qp_y; // QP_X, QP_Y
	        private Double pt_x, pt_y; //QP_X, QP_Y
	        private DoubleWritable final_distance = new DoubleWritable(1);	//key
	        private Text final_point = new Text();	//value
	        
	        @Override
	        public void setup(Context context) throws IOException, InterruptedException {
	            Configuration conf=context.getConfiguration();
	            qp_x = conf.get("QP_X");
	            qp_y = conf.get("QP_Y");
	            pt_x = Double.parseDouble(qp_x);
	            pt_y = Double.parseDouble(qp_y);
	        }
	        
	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	            
	            String org_str = value.toString();
	            String[] split_string = org_str.split(",");
	            double data_pt_x = Double.parseDouble(split_string[1]);
	            double data_pt_y = Double.parseDouble(split_string[2]);
	            Double temp_dist = (Math.sqrt(Math.pow((pt_x-data_pt_x),2) + Math.pow((pt_y-data_pt_y),2)));
	            final_distance.set(temp_dist);
	            final_point.set(org_str);
	            context.write(final_distance, final_point);
	        }
	    }
	 
	 public static class KNNReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
	        private int limit = 1;
	        
	        public void reduce(DoubleWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
	            Configuration conf = context.getConfiguration();
	            int k = conf.getInt("k", 1);
	            if (limit <= k) {
	                for (Text text : value) {
	                    context.write(key, text);
	                    limit++;
	                }
	            }
	        }
	    }

    public static void main( String[] args )throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = new Configuration();
        conf.set("QP_X",args[1]);
        conf.set("QP_Y",args[2]);
        conf.setInt("k", Integer.parseInt(args[3]));
        Job job = Job.getInstance(conf, "KNN");
        job.setJarByClass(KNN.class);
        job.setMapperClass(KNNMapper.class);
	job.setCombinerClass(KNNReducer.class);        
	job.setReducerClass(KNNReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(1);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
	
        FileSystem fs = FileSystem.get(conf);
        if(!fs.exists(new Path("hdfs://localhost:9000"+args[0]))){
        	System.out.println("Input File Doesn't exist");
        	return;
        }
        		
        if(!fs.exists(new Path("hdfs://localhost:9000"+args[4]))){
        	System.exit(job.waitForCompletion(true) ? 0 : 1);	
        }else {
        	System.out.println("Output Directory Already Exists");
        	return;
        }
	
        

    }

}
