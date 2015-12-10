package dota.statistics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.opencsv.CSVParser;

public class Stat {
	public static class GameMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
			JSONObject matchDetails;
			try {
				matchDetails = new JSONObject(value.toString());
			} catch (JSONException je) {
				return;
			}
			
			JSONArray players = matchDetails.getJSONObject("result").getJSONArray("players");
			boolean radWin = matchDetails.getJSONObject("result").getBoolean("radiant_win");
			if(players == null || players.length() < 10) {
				return;
			}
			
			for(int i = 0; i < 5; i++) {
				JSONObject player = players.getJSONObject(i);
				if(player.optInt("leaver_status", -1) != 0) {
					continue;
				}
				int killsAssists = player.optInt("kills") + player.optInt("assists");
				int deaths = player.optInt("deaths");
				double KDA = killsAssists / (deaths == 0 ? 1 : deaths);
				int lastHits = player.optInt("last_hits");
				IntWritable heroId = new IntWritable();
				heroId.set(player.optInt("hero_id"));
				Text attr = new Text();
				StringBuilder output = new StringBuilder();
				output.append(Double.toString(KDA)).append(",")
				.append(Integer.toString(lastHits)).append(",")
				.append(Boolean.toString(radWin));
				attr.set(output.toString());
				context.write(heroId, attr);
			}
			
			for(int i = 5; i < 10; i++) {
				JSONObject player = players.getJSONObject(i);
				if(player.optInt("leaver_status", -1) != 0) {
					continue;
				}
				int killsAssists = player.optInt("kills") + player.optInt("assists");
				int deaths = player.optInt("deaths");
				double KDA = killsAssists / (deaths == 0 ? 1 : deaths);
				int lastHits = player.optInt("last_hits");
				IntWritable heroId = new IntWritable();
				heroId.set(player.optInt("hero_id"));
				Text attr = new Text();
				StringBuilder output = new StringBuilder();
				output.append(Double.toString(KDA)).append(",")
				.append(Integer.toString(lastHits)).append(",")
				.append(Boolean.toString(!radWin));
				attr.set(output.toString());
				context.write(heroId, attr);
			}
		}
	}
	
	public static class GameReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		int total;
		double KDA;
		int wins;
		long totalLastHits;
		IntWritable heroId;
		public void setup (Context context) {
			total = 0;
			KDA = 0d;
			wins = 0;
			totalLastHits = 0;
		}
		
		public void reduce (IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			heroId = key;
			CSVParser csvParser = new CSVParser(',');
			for(Text record : values) {
				//System.out.println(record.toString());
				String[] records = csvParser.parseLine(record.toString());
				KDA += Double.parseDouble(records[0]);
				totalLastHits += Integer.parseInt(records[1]);
				wins += (Boolean.parseBoolean(records[2]) ? 1 : 0);
				++total;
			}
		}
		
		public void cleanup (Context context) throws IOException, InterruptedException {
			Text record = new Text();
			StringBuilder output = new StringBuilder();
			output.append(Integer.toString(wins)).append(", ")
				.append(Integer.toString(total)).append(", ")
				.append(Double.toString((double)totalLastHits/total)).append(", ")
				.append(Double.toString(KDA/total));
			record.set(output.toString());
			context.write(heroId, record);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Stat <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Stat");
		job.setJarByClass(Stat.class);
		job.setMapperClass(GameMapper.class);
		job.setReducerClass(GameReducer.class);
		job.setNumReduceTasks(113);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		Path outFile = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outFile);
		
		if (job.waitForCompletion(true)) {
			/*
			 * second MapReduce job to get result from all 10 files
			 * and to calculate the average
			 */
//			Job averageJob = Job.getInstance(conf, "Average");
//			averageJob.setJarByClass(AirlineDelay.class);
//			averageJob.setMapperClass(AverageMapper.class);
//			averageJob.setReducerClass(AverageReducer.class);
//			averageJob.setNumReduceTasks(1);
//			averageJob.setMapOutputKeyClass(NullWritable.class);
//			averageJob.setOutputKeyClass(NullWritable.class);
//			averageJob.setOutputValueClass(Text.class);
//			FileInputFormat.setInputPaths(averageJob, new Path(args[1]));
//			FileOutputFormat.setOutputPath(averageJob, new Path(args[2]));
//			
//			System.exit(averageJob.waitForCompletion(true) ? 0 : 1);
			System.exit(0);
		} else {
			System.exit(1);
		}
		
		System.exit(0);
	}
}
