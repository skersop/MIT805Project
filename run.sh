#!/bin/bash

# This script controls the overall process flow
# From here, we compile our app scripts and trigger each task
# Takes the following arguments:
#	-i	text	Input path	Path of initial input data file
#	-c	bit	Compile		Whether .java files should be recompiled

# TODO: 
#	Arguments for which tasks to run
#	Check whether tasks ran successfully

# Handle argument passing
while getopts d:c: flag
do
    case "${flag}" in
        d) INPUT_PATH=${OPTARG};;
        c) COMPILE=${OPTARG};;
    esac
done
echo "Input path: $INPUT_PATH";
echo "Compile: $COMPILE";

HADOOP="$( which hadoop )"

TASK1="GetSmartphoneUserSessions"

# Compile code if -c 1
if [ $COMPILE = 1 ]
then
	echo "========================================"
	echo "----- Compiling java scripts -----"
	echo "Compiling ${TASK1}.java"
	${HADOOP} com.sun.tools.javac.Main ${TASK1}.java
	jar cf ${TASK1}.jar ${TASK1}*.class
fi

# Delete existing outputs
echo "========================================"
echo "----- Deleting existing outputs -----"
rm -r ${TASK1}

# Run hadoop
echo "========================================"
echo "----- Running jobs -----"
${HADOOP} jar ${TASK1}.jar ${TASK1} ${INPUT_PATH} ${TASK1}

# Print results

echo "========================================"
echo "----- Printing results -----"
cat ${TASK1}/*





