package org.apache.hadoop.examples;
//package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {

   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
     private final static IntWritable one = new IntWritable(1);
     private Text word = new Text();

     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       String line = value.toString();
       System.out.print("========>Map input key: " + key.toString() + ", value: " + line + "\n");
       StringTokenizer tokenizer = new StringTokenizer(line);
       while (tokenizer.hasMoreTokens()) {
         word.set(tokenizer.nextToken());
         output.collect(word, one);
         System.out.print("-------->Map output word: " + word + ", one: " + one + "\n");
       }
       
     }
   }

   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
    	 System.out.print("========>Reduce input key: " + key.toString() + "\n");
    	 int sum = 0;
       while (values.hasNext()) {
         int v = values.next().get();
         sum += v;
         System.out.println("--->  v: " + v);
       }
       output.collect(key, new IntWritable(sum));
       System.out.println("------>Reduce output key: " + key.toString() + ", sum: " + sum);
     }
   }

   public static void main(String[] args) throws Exception {
     JobConf conf = new JobConf(WordCount.class);
     conf.setJobName("wordcount");

     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(IntWritable.class);

     conf.setMapperClass(Map.class);
     //conf.setCombinerClass(Reduce.class);
     //conf.setReducerClass(Reduce.class);

     conf.setInputFormat(TextInputFormat.class);
     conf.setOutputFormat(TextOutputFormat.class);

     FileInputFormat.setInputPaths(conf, new Path(args[0]));
     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
     
     //// delete the output directory if it already exists
     FileSystem fs = FileSystem.get(new Configuration());
     fs.delete(new Path(args[1]), true);

     JobClient.runJob(conf);
   }
}