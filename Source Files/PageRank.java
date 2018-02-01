package pagerank;

import java.io.IOException;              //Required Import statements are imported
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRank extends Configured implements Tool{					//Main class
	
	public static final Pattern titlePattern = Pattern.compile(".*<title>(.*?)</title>.*");
	public static final Pattern textPattern = Pattern.compile(".*<text.*?>(.*?)</text>.*");	
	public static final Pattern linkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
	
	
	public static int tempCount = 1;				// for Counting the number of iterations of page rank calculations
	public static double dampingFactor = 0.85;		//Considering  damping factor as 0.85 for calculation of page rank
	public static int max = 10;						// keeping the maximum number of iterations Limit as 10.
	
	//Main function
	public static void main(String args[]) throws Exception{
		int res = ToolRunner.run(new PageRank(), args);			//Calling the run function of job 1
		  System.exit(res);
	}
	
	
	
	@Override
	public int run(String[] args) throws Exception {
		
				
		Job job1=Job.getInstance(getConf(),"Rank1");   //job1 is for the extraction of  info from XML files.
		job1.setJarByClass(this.getClass());
				
		FileInputFormat.addInputPath(job1, new Path(args[0]));    // input path
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "Temp/IntermediateOutput" + tempCount));   // output path
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		if(job1.waitForCompletion(true)) {			
			ToolRunner.run(new PageRankCalculation(), args);		// Waits until the completion of Job 1  and then calls the Job2 run function.
			
		} 
		

		return 0;
	}

	 
		public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {		//Map1 Phase
			@Override
	        public void map(LongWritable off, Text Text, Context context) throws IOException, InterruptedException {
				
	            String line = Text.toString();					//converts the text  to string
	            if(line.length() > 0) {
	                Matcher titleMatPat = PageRank.titlePattern.matcher(line);	//matches the pattern to extract the title of the input file
	                StringBuilder titleBuilder = new StringBuilder();
	                if(titleMatPat.matches()) {
	                    titleBuilder.append(titleMatPat.group(1).toString());
	                }
	                Matcher textMatPat = PageRank.textPattern.matcher(line);
	               
	                if(textMatPat.matches()) {                    
	                    String outLinks = textMatPat.group(1).toString();		//matches the pattern to extract the text of the input file
	                    
	                    Matcher linkMatPat = PageRank.linkPattern.matcher(outLinks);
	                    while(linkMatPat.find()) {
	                        int i = 0;
	                        while(i < linkMatPat.groupCount()) {
	                            
	                            context.write(new Text(linkMatPat.group(i + 1).toString().trim()), new Text(""));
	                            context.write(new Text(titleBuilder.toString().trim()), new Text(linkMatPat.group(i + 1).toString()));       //writes the outlinks
	                            i++;
	                        }
	                    }
	                }
	            }
	        }
	    }
		
	
		public static class Reduce1 extends Reducer<Text, Text, Text, Text> {         //Reduce 1 Phase
			@Override
			public void reduce(Text key, Iterable<Text> Links, Context context) throws IOException, InterruptedException {
	            int outlinks = 0;
	            StringBuilder linkBuilder = new StringBuilder("");
	            for(Text Text : Links) {
	                if(Text.toString().length() > 0) {
	                    linkBuilder.append("#####" + Text.toString());        //appending  delimiter which is used for seperation  in the later stages
	                    outlinks++;
	                }
	            }
	            String val = "Links-----" + outlinks + linkBuilder.toString().trim();
	            context.write(key, new Text(val));
	        }
	    }
}