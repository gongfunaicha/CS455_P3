CS455 Homework 3: Programming Component
United States Census Data Analysis Using MapReduce

Compile instruction:
use "ant" command to compile

Sample execution command:
$HADOOP_HOME/bin/hadoop jar dist/CS455_HW3.jar cs455.hadoop.run1.Run1Job /data/census /home/output
python VisualAnalysis.py <file (path) for analysis>

File descriptions:
cs455/
	hadoop/
		run1/
			Run1Combiner.java: Combiner class, is responsible for combining all the mapper output of one state of one mapper into one output to reduce bandwidth usage
			Run1Job.java: Main class, used to provide the all the information that will be used by mapreduce and start the map reduce job
			Run1Mapper.java: Mapper class, is responsible for extracting data from the given segment and produce one output for each segment
			Run1Reducer.java: Reducer class, collects all the data and perform analysis to solve the questions
		util/
			objects/
				AgeCountPair.java: Helper class that store a pair of age range and count, used in renter age distribution analysis
				AgeDistributionObject.java: Object that stores the number of male and female in each age range
				ElderCountObject.java: Object that stores the number of elder and total number of people
				HousePositionCountObject.java: Object that stores the number of houses in urban, rural, and other areas
				HouseValueCountObject.java: Object that stores the number of houses in each value range
				MarriageCountObject.java: Object that stores the number of never married count and total count of male and female
				RentCountObject.java: Object that stores the number of houses within each rent range
				RenterAgeDistributionObject: Object that stores the number of renters in each renter age category
				ResidenceCountObject.java: Object that stores the number of rent house and owned house
				RoomCountObject.java: Object that stores the number of houses based on the number of rooms in the house
			writable/
				AgeDistributionCountWritable: Writable version of age distribution object
				ElderCountWritable: Writable version of elder count object
				HousePositionCountWritable: Writable version of house position count object
				HouseValueCountWritable: Writable version of house value count object
				MarriageCountWritable: Writable version of marriage count object
				RentCountWritable: Writable version of rent count object
				RenterAgeDistributionWritable: Writable version of renter age distribution object
				ResidenceCountWritable: Writable version of residence count object
				RoomCountWritable: Writable version of room count object
				Run1CombinedWritable: Main writable class, includes variables of all the writables above, used to transfer information from mapper to reducer
			DataExtractor.java: Class containing only static methods that are resonsible for extracting data from the segment, used by mapper
VisualAnalysis.py: Python file that produces pie chart of distribution of US renter ages based on the output generated by map reduce. Need to first pull the output from the HDFS to local disk before using this program.


Things to note:
1. It seems that data of two states (PR and VI) in the dataset are incorrect, so I implemented special handling when data seems incorrect (for example, there is no house in the state), especially for Q7 and Q8. For Q7, states without houses are not included in the analysis for 95th percentile. For Q8, state without any people are not included in the final analysis for "highest percentage of elderly people".


Explanation for Q9:
As a start-up firm, the efficiency of the money spent is critical. So I decided to use the dataset to figure out people of which age are the most probable to rent a house. By figuring out the largest three age groups that will rent a house, we can make them the target of our advertisements of renting a house to maximize our profit from the advertisement campaign.
The attached VisualAnalysis.py will produce a pie chart of distribution of US renter ages to facilitate making decision of which age group to target.