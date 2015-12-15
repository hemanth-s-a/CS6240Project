package dota.statistics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
			int duration = matchDetails.getJSONObject("result").getInt("duration");
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
				int heroDamage = player.getInt("hero_damage");
				int towerDamage = player.getInt("tower_damage");
				int gold = player.getInt("gold_per_min");
				IntWritable heroId = new IntWritable();
				heroId.set(player.optInt("hero_id"));
				Text attr = new Text();
				StringBuilder output = new StringBuilder();
				output.append(Double.toString(KDA)).append(",")
				.append(Integer.toString(lastHits)).append(",")
				.append(Boolean.toString(radWin)).append(",")
				.append(Integer.toString(duration)).append(",")
				.append(heroDamage).append(",")
				.append(towerDamage).append(",")
				.append(gold);
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
				int heroDamage = player.getInt("hero_damage");
				int towerDamage = player.getInt("tower_damage");
				int gold = player.getInt("gold_per_min");
				IntWritable heroId = new IntWritable();
				heroId.set(player.optInt("hero_id"));
				Text attr = new Text();
				StringBuilder output = new StringBuilder();
				output.append(Double.toString(KDA)).append(",")
				.append(Integer.toString(lastHits)).append(",")
				.append(Boolean.toString(!radWin)).append(",")
				.append(Integer.toString(duration)).append(",")
				.append(heroDamage).append(",")
				.append(towerDamage).append(",")
				.append(gold);
				attr.set(output.toString());
				context.write(heroId, attr);
			}
		}
	}
	
	public static class GameReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		int total;
		double KDA;
		int wins;
		long totalLastHits;
		int[] timeWins;
		int[] timeTotal;
		long towerDamage;
		long heroDamage;
		long gold;
		IntWritable heroId;
		public void setup (Context context) {
			total = 0;
			KDA = 0d;
			wins = 0;
			totalLastHits = 0;
			towerDamage = 0;
			heroDamage = 0;
			gold = 0;
			timeWins = new int[5];
			timeTotal = new int[5];
			for(int i = 0; i < 5; i++) {
				timeWins[i] = 0;
				timeTotal[i] = 0;
			}
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
				heroDamage += (Integer.parseInt(records[4]));
				towerDamage += (Integer.parseInt(records[5]));
				gold += (Integer.parseInt(records[6]));
				switch((int)Integer.parseInt(records[3])/900) {
				case 0:
					timeWins[0] += (Boolean.parseBoolean(records[2]) ? 1 : 0);
					timeTotal[0] += 1;
					break;
				case 1:
					timeWins[1] += (Boolean.parseBoolean(records[2]) ? 1 : 0);
					timeTotal[1] += 1;
					break;
				case 2:
					timeWins[2] += (Boolean.parseBoolean(records[2]) ? 1 : 0);
					timeTotal[2] += 1;
					break;
				case 3:
					timeWins[3] += (Boolean.parseBoolean(records[2]) ? 1 : 0);
					timeTotal[3] += 1;
					break;
				default:
					timeWins[4] += (Boolean.parseBoolean(records[2]) ? 1 : 0);
					timeTotal[4] += 1;
					break;
				}
			}
		}
		
		public void cleanup (Context context) throws IOException, InterruptedException {
			Text record = new Text();
			StringBuilder output = new StringBuilder();
			if(heroId == null) {
				return;
			}
			output.append(heroId.get()).append(", ")
				.append(Integer.toString(wins)).append(", ")
				.append(Integer.toString(total)).append(", ")
				.append(Double.toString((double)wins*100/total)).append(", ")
				.append(Double.toString((double)totalLastHits/total)).append(", ")
				.append(Double.toString(KDA/total)).append(", ");
			for(int i = 0; i < 5; i++) {
				output.append(timeWins[i]).append(", ").append(timeTotal[i]).append(", ");
				output.append(Double.toString((double)timeWins[i]*100/timeTotal[i])).append(", ");
			}
			output.append((double)heroDamage/total).append(", ")
				.append((double)towerDamage/total).append(", ")
				.append((double)gold/total);
			record.set(output.toString());
			context.write(NullWritable.get(), record);
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
		job.setOutputKeyClass(NullWritable.class);
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
