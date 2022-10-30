
HADOOP=$1
TASK=$2
INPUT_PATH=$3


echo "----- Compiling script -----"
${HADOOP} com.sun.tools.javac.Main ${TASK}.java
jar cf ${TASK}.jar ${TASK}*.class

echo "----- Deleting existing outputs -----"
rm -r ${TASK}

echo "----- Running job -----"
${HADOOP} jar ${TASK}.jar ${TASK} ${INPUT_PATH} ${TASK}


