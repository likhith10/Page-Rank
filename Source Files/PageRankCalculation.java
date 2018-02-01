package pagerank;


import java.io.File;				//Required Import statements are imported
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class PageRankCalculation extends Configured implements Tool {			//Main Class

	boolean steadyst= false;
	
		
	@Override
	public int run(String[] arg0) throws Exception {
		
		
		pagerankcalc(arg0);	      //Calls the  page rank calculations Function where the page rank is calculated iteratively
				
		try {
			ToolRunner.run(new PageRankCleanUp(), arg0);  //PageRankCleanUp class is called  after the second job is completed
		} catch (Exception e) {
			// TODO: handles exceptions
		}
		
		return 0;
	}
	
	public void pagerankcalc(String[] args) throws Exception{
	
		while(PageRank.tempCount<PageRank.max){			//Loops till  we reach a steady state otherwise till the max number of iterations are reached 
			Configuration conf = getConf();	
		    Job job2;														//Job2 is created
				
        	job2 = Job.getInstance(conf, "Rank1" + PageRank.tempCount);
        
            job2.setJarByClass(this.getClass());
           
            
            String inputPath  = args[1] +  "Temp/IntermediateOutput" + PageRank.tempCount;			//Input Path
    		String outputPath = args[1] +  "Temp/IntermediateOutput" + (PageRank.tempCount + 1);       //Creating a temporary path for the outputs of individual iterations.
            
    		  		
    		FileInputFormat.addInputPath(job2, new Path(inputPath));
    		FileOutputFormat.setOutputPath(job2, new Path(outputPath));
    		
    		job2.setMapperClass(Map2.class);			 
    		job2.setReducerClass(Reduce2.class);			
    		job2.setOutputKeyClass(Text.class);
    		job2.setOutputValueClass(Text.class);    		
    		job2.waitForCompletion(true);	
    				
    		File file1=new File(inputPath + "/part-r-00000");	
    		File file2=new File(outputPath + "/part-r-00000");
    		if(FileUtils.contentEquals(file1, file2)) //Comparing the immediate output files of iterations,
    			break;  // if there is no change, this means page rank values are Converged so the  next iterations will be stopped .
    		
    		PageRank.tempCount++;   //Otherwise next iterations will be continued till max limit 0f 10
               
        }
				
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {   //Map2 Phase
	
		public void map(LongWritable off, Text Text1, Context context) throws IOException, InterruptedException {
			try {
                String line = Text1.toString();
                 
                if(line.length() > 0) {
                    String[] lineSplit = line.split("\\t");   //Split the input after the tab
                    String keyText = lineSplit[0];
                    String valueText = lineSplit[1];
                    String[] valueSplit = valueText.trim().split("-----");
                    
                    if(valueSplit.length > 1) {
                        if(valueSplit[0].equals("Links")) {
                            
                        	int nunodes = Integer.parseInt((valueSplit[1].split("#####"))[0]);
                        	                     
                        	
						context.write(new Text(keyText.trim()), new Text((1/(double) nunodes) + ""));
                        } else {
                            double pageRank = Double.parseDouble(valueSplit[0].trim());
                            String[] links = valueSplit[1].split("#####");
                            int numberOfLinks = Integer.parseInt(links[0].trim());
                            if(numberOfLinks > 0) {
                                double pageRankPercent = (double) pageRank / numberOfLinks;
                                for(int linkCount = 1; linkCount < links.length; linkCount++) {
                                	
                                    context.write(new Text(links[linkCount]), new Text(pageRankPercent + ""));
                                }
                            }
                        }
                        context.write(new Text(lineSplit[0]), new Text(lineSplit[1]));
                    }
                }
            } catch (Exception E) {
                E.printStackTrace();
            }
        }
		
	}
	
	
    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {     //Reduce2 phase
		@Override
		public void reduce(Text key, Iterable<Text> AllLinks, Context context) throws IOException, InterruptedException {
            double valuepageRank = 0;
			
			StringBuffer linksBuffer = new StringBuffer("");
			    
			for (Text link : AllLinks) {
				
				if(link.toString().length() > 0) {
					if(link.toString().contains("-----")) {
						// Splitting the link with delimiter	 
						String links[] = link.toString().split("-----");
						//overallPageRank
						linksBuffer.append(links[1]);
					}
					else {
						valuepageRank = valuepageRank + Double.parseDouble(link.toString());
					} 
				}
			} 
			valuepageRank = valuepageRank + (1 - PageRank.dampingFactor);       //pagerank calculation
			String value = valuepageRank + "-----" + linksBuffer.toString();
		;
			context.write(key, new Text(value));          
        }
    }

}
