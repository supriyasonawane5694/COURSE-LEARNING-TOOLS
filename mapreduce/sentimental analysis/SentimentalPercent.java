
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SentimentalPercent {
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
        
		
		private Map<String, Integer> dictMap = new HashMap<String, Integer>();
		
		private Text outputKey = new Text();
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		    
		    FileSystem fs = FileSystem.get(context.getConfiguration());		    
		
			if (p.getName().equals("AFINN.txt")) {
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(p)));

					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split("\t");
						String string = tokens[0];
						Integer value = Integer.parseInt(tokens[1]);
						dictMap.put(string, value);
						line = reader.readLine();
					}
					reader.close();
				}

		
			
			if (dictMap.isEmpty()) {
				throw new IOException("MyError:Unable to load dictionary data.");
			}

		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        	Integer sentValue = 0;
        	try{
	        	 StringTokenizer strTknr = new StringTokenizer(value.toString());
	        	 while(strTknr.hasMoreTokens()){
	        		 String token = strTknr.nextToken().trim().toLowerCase();
	        		 sentValue = dictMap.get(token);
	        		 if(sentValue != null){
	        			 if(sentValue > 0)
	        			 {
	        				 outputKey.set("Positive");
	        			 }
	        			 else {
	        				 outputKey.set("Negative");
	        				 sentValue = sentValue * -1;
	        			 }
	        		 }
	        		 else
	        		 {
	        			 //neutral
	        			 outputKey.set("Positive");
	        			 sentValue = 0;
	        		 }
	        	 }
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
        	
        	
      	  	context.write(outputKey,new IntWritable(sentValue));
        }  
        
 
        

}
	
    public static class ReduceClass extends Reducer<Text,IntWritable,NullWritable,Text>
	   {	
    	 Double sentpercent = 0.00, pos_total = 0.0 ,neg_total = 0.0 ;
		    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
				
		    	Double sum = 0.0;
		         for (IntWritable val : values)
		         { 
		        	 sum += val.get();
		         }
	        	if(key.toString().equals("Positive"))
	        	{
	        		pos_total = sum;
	        	}
	        	if(key.toString().equals("Negative"))
	        	{
	        		neg_total = sum;
	        	}
		    }
		    
	        protected void cleanup(Context context)throws java.io.IOException, InterruptedException{
	       	sentpercent = ((pos_total - neg_total)/(pos_total + neg_total))* 100;
	        	String str = "Sentiment percent for the given text is - " + sentpercent;
	        	context.write(NullWritable.get(), new Text(str));
	        }
	   }
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
	Configuration conf = new Configuration();
//	conf.set("mapreduce.output.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(SentimentalPercent.class);
    job.setJobName("Map Side Join, sentimental analysis");
    job.setMapperClass(MyMapper.class);
    job.addCacheFile(new Path(args[1]).toUri());
    job.setReducerClass(ReduceClass.class);
  //  job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    
    job.waitForCompletion(true);
    
    
  }
}
