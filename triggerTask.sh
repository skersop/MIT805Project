
HADOOP="$( which hadoop )"
TASK=$1
INPUT_PATH=$2


echo "----- Compiling script -----"
${HADOOP} com.sun.tools.javac.Main ${TASK}.java
jar cf ${TASK}.jar ${TASK}*.class

echo "----- Deleting existing outputs -----"
rm -r ${TASK}

echo "----- Running job -----"
${HADOOP} jar ${TASK}.jar ${TASK} ${INPUT_PATH} ${TASK}

echo "----- Printing results -----"
cat ${TASK}/*

echo "========================================"
echo "---------------- Cleanup ----------------"
echo "========================================"
rm *.class
rm *.jar
