#!/bin/bash

# This script controls the overall process flow

INPUT_PATH=$1;
HADOOP="$( which hadoop )"

echo "Input path: ${INPUT_PATH}";
echo "Hadoop path: ${HADOOP}";

TASK1="MeanSmartphoneBrandCost"
TASK2="GetSmartphoneUserSessionIDs"
TASK3="FilterSmartphoneUserSessions"
TASK4="CountCategoryCodes"
TASK5="CountBrands"
TASK6="SmartphoneTimes"
TASK7="PopularPeakCategories"

bash ./triggerTask.sh ${HADOOP} ${TASK1} ${INPUT_PATH}
cat ${TASK1}/* &> "${TASK1}.txt"
rm -r ${TASK1}

bash ./triggerTask.sh ${HADOOP} ${TASK2} ${INPUT_PATH}
cat ${TASK2}/* &> "${TASK2}.txt"
rm -r ${TASK2}

bash ./triggerTask.sh ${HADOOP} ${TASK3} ${INPUT_PATH}
cat ${TASK3}/* &> "${TASK3}.txt"
rm -r ${TASK3}

bash ./triggerTask.sh ${HADOOP} ${TASK4} "${TASK3}.txt"
cat ${TASK4}/* &> "${TASK4}.txt"
rm -r ${TASK4}

bash ./triggerTask.sh ${HADOOP} ${TASK5} "${TASK3}.txt"
cat ${TASK5}/* &> "${TASK5}.txt"
rm -r ${TASK5}

bash ./triggerTask.sh ${HADOOP} ${TASK6} ${INPUT_PATH}
cat ${TASK6}/* &> "${TASK6}.txt"
rm -r ${TASK6}

javac PeakHours.java
jar cf PeakHours.jar PeakHours*.class
java PeakHours

bash ./triggerTask.sh ${HADOOP} ${TASK7} ${INPUT_PATH}
cat ${TASK7}/* &> "${TASK7}.txt"
rm -r ${TASK7}

echo "========================================"
echo "---------------- Cleanup ----------------"
echo "========================================"
rm *.class
rm *.jar

