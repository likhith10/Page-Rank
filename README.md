# Page-Rank
Source files:
1)PageRank.java
2)PageRankCalculation.java
3)PageRankCleanUp.java

Input file: wiki-micro.txt

PageRank.Java
In this class The patterns are matched so as to extract the information form the title and the text part of the xml file by using the title pattern and text pattern 

PageRankCalculation.java
This class implements PageRankCalculation where the page rank is calculated iteratively till  we reach a steady state . Steady state is known by Comparing the immediate output files of iterations, if there is no change, this means page rank values are Converged so the  next iterations will be stopped .  If Steady state is not reached the iterations will be stopped when the tempcount reaches 10 as the max limit to the iterations is set at 10.

PageRankCleanUp.java 
In this class  the output of the last iteration is taken as Input  and  the results are sorted  in the decreasing order of the page rank values.The Iterative output files generated during pagerank calculation will be deleted.

The Main Class in the project is PageRank class.

Steps For Execution:
1) In Eclipse,Add the Required External jar files to the  project.
2)Right click on the project.Click on the Run As configuration.
3)Select PageRank Class as Main class.
4)In Arguments Section.Set the input and output arguments as "/home/cloudera/Inputs/input/*" "/home/cloudera/Inputs/input/Finaloutput"       	 (Here the input is wiki-micro.txt)
5)click Apply and Run the project.
6)In the output location We can find the output folder with the output file.



All the Source files,file Output and README file is included in lchinnam.zip
