#!/bin/bash

# This script controls the overall process flow
# From here, we compile our app scripts and trigger each task
# Takes the following arguments:
#	-i	text	Input path	Path of initial input data file
#	-c	bit	Compile		Whether .java files should be recompiled

# TODO: 
#	Arguments for which tasks to run
#	Check whether tasks ran successfully
foo=1;
# Handle argument passing
while getopts d:c flag
do
    case "${flag}" in
        d) INPUT_PATH=${OPTARG};;
        c) COMPILE='true';
    esac
done
echo "Input path: $INPUT_PATH";
echo "Compile: $COMPILE";

HADOOP="$( which hadoop )"

TASK1="GetSmartphoneUserSessionIDs"
TASK2="FilterSmartphoneUserSessions"


echo "========================================"
echo "---------------- Task 1 ----------------"
echo "========================================"
if [ $COMPILE = 'true' ]
then
	echo "----- Compiling script -----"
	${HADOOP} com.sun.tools.javac.Main ${TASK1}.java
	jar cf ${TASK1}.jar ${TASK1}*.class
fi

echo "----- Deleting existing outputs -----"
rm -r ${TASK1}

echo "----- Running job -----"
${HADOOP} jar ${TASK1}.jar ${TASK1} ${INPUT_PATH} ${TASK1}

echo "----- Saving output -----"
cat ${TASK1}/* &> "sessionids.txt"

echo "----- Printing results -----"
cat ${TASK1}/*

echo "----- Deleting intermediate outputs -----"
rm -r ${TASK1}

echo "========================================"
echo "---------------- Task 2 ----------------"
echo "========================================"
if [ $COMPILE = 'true' ]
then
	echo "----- Compiling script -----"
	${HADOOP} com.sun.tools.javac.Main ${TASK2}.java
	jar cf ${TASK2}.jar ${TASK2}*.class
fi

echo "----- Deleting existing outputs -----"
rm -r ${TASK2}

echo "----- Running job -----"
${HADOOP} jar ${TASK2}.jar ${TASK2} ${INPUT_PATH} ${TASK2}

echo "----- Printing results -----"
cat ${TASK2}/*





