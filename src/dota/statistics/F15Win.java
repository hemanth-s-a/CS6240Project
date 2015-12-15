package dota.statistics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVParser;

public class F15Win {
	//static int arrayIndex;
	public static class GameMapper extends Mapper<Object, Text, DoubleWritable, IntWritable> {
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
			//System.out.println(value.toString());
			if(value.toString() == null) {
				return;
			}
			CSVParser csvParser = new CSVParser(',');
			String[] record;
			try {
				record = csvParser.parseLine(value.toString());
			} catch (IOException e) {
				return;
			}
			
			System.out.println(record);
			
			int heroId = Integer.parseInt(record[0]);
			double winRate = Double.parseDouble(record[context.getConfiguration().getInt("arrayIndex", -1)]);
			
			DoubleWritable mapKey = new DoubleWritable();
			mapKey.set(winRate);
			IntWritable mapValue = new IntWritable();
			mapValue.set(heroId);
			try {
			context.write(mapKey, mapValue);
			} catch (IOException ioe) {
				System.out.println(heroId + ", " + winRate);
				System.out.println(ioe.getLocalizedMessage());
				System.exit(1);
			}
		}
	}
	
	public static class GameReducer extends Reducer<DoubleWritable, IntWritable, NullWritable, Text> {
		public void reduce (DoubleWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Text record = new Text();
			StringBuilder output = new StringBuilder();
			for(IntWritable value : values) {
				output.append(value.get()).append(", ")
					.append(key.get()).append(", ");
			}
			record.set(output.toString());
			context.write(NullWritable.get(), record);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: F15Win <in> <out>");
			System.exit(2);
		}
		conf.setInt("arrayIndex", Integer.parseInt(args[2]));
		//arrayIndex = Integer.parseInt(args[2]);
		Job job = Job.getInstance(conf, "F15Win");
		job.setJarByClass(Stat.class);
		job.setMapperClass(GameMapper.class);
		job.setReducerClass(GameReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		Path outFile = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outFile);
		
		if (job.waitForCompletion(true)) {
			System.exit(0);
		} else {
			System.exit(1);
		}
		
		System.exit(0);
	}
}
