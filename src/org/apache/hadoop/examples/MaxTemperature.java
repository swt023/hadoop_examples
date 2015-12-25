package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperature {
	public static class MaxTemperatureMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable lkey, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			RecordParser parser = new RecordParser();
			parser.parse(line);

			if (parser.isValidTemperature()) {
				context.write(new Text(parser.getYear()), new IntWritable(
						parser.getTemperature()));
			} else {
				context.setStatus("Bad record, pleass see logs");
				context.getCounter(Temperature.bad_data).increment(1);
			}
		}

	}

	public static class MaxTemperatureReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				if (value.get() > maxValue) {
					maxValue = value.get();
				}
			}
			context.write(key, new IntWritable(maxValue));
		}
	}

	enum Temperature {
		bad_data,
		over_100
	}
	
	// Parser pick out the year&temperature from record line
	public static class RecordParser {
		private static int MISSING_TEMPERATURE = 9999;
		private String year;
		private int temperature;
		private String quality;
		private boolean valid = true;

		public void parse(String record) {
			try {
				year = record.substring(15, 19);
				String temp;
				//Thread.sleep(30*1000);

				if (record.charAt(87) == '+') {
					temp = record.substring(88, 92);
				} else {
					temp = record.substring(87, 92);
				}
				
				temperature = Integer.parseInt(temp);

				quality = record.substring(92, 93);
				System.out.println("temp: " + temp + ", quality: " + quality);
			} catch (Exception e) {
				valid = false;
				System.err.println("Bad record: " + record);
			}
		}

		public void parse(Text record) {
			parse(record.toString());
		}

		public boolean isValidTemperature() {
			return valid
					&& (temperature != MISSING_TEMPERATURE && quality
							.matches("[012459]"));
		}

		public String getYear() {
			return year;
		}

		public int getTemperature() {
			return temperature;
		}

	}

	// driver class to run the mapper reducer funtions
	public static class MaxTemperatureDriver extends Configured implements Tool {

		@Override
		public int run(String[] args) throws Exception {
			// TODO Auto-generated method stub
			if (args.length != 2) {
				System.err.printf(
						"Usage: %s [generic options] <input> <output>",
						getClass().getSimpleName());
				ToolRunner.printGenericCommandUsage(System.err);
				return -1;
			}
			
			Configuration conf = new Configuration();
			//conf.set("fs.default.name", "hdfs://localhost:9000");
			//conf.set("mapred.job.tracker", "local");
			this.setConf(conf);
		

			Job job = new Job(getConf(), "Max Temperature");
			job.setJarByClass(getClass());

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setMapperClass(MaxTemperatureMapper.class);
			job.setCombinerClass(MaxTemperatureReducer.class);
			job.setReducerClass(MaxTemperatureReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileSystem fs = FileSystem.get(getConf());
			System.out.println("conf fs.default.name: " + getConf().get("fs.default.name"));
			System.out.println("mapred.job.tracker: " + getConf().get("mapred.job.tracker"));
			fs.delete(new Path(args[1]), true);

			//job.setNumReduceTasks(2);
			return job.waitForCompletion(true) ? 0 : 1;
		}
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
		System.exit(exitCode);
	}
}
