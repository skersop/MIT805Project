# Script to go through the compile and run process of the WordCount example from
# https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
# Used to test hadoop installation

# Compile code
/home/stephan/hadoop-3.3.4/bin/hadoop com.sun.tools.javac.Main CategoryCount.java
jar cf wc.jar CategoryCount*.class

# Delete existing output
rm -r output

# Run hadoop
/home/stephan/hadoop-3.3.4/bin/hadoop jar wc.jar CategoryCount test.csv output

# Print results
cat output/*
