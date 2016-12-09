hadoop fs -rmr OverSample
hadoop fs -put data/sampleFolder/OverSample OverSample

hadoop jar mn.jar bayes.NBTrainDriver -D delimiter="," -D input="OverSample" -D output="model" -D continousVariables="2,3,4,5,6,7,8" -D discreteVariables="1" -D targetVariable="9"  -D numColumns="9" 


hadoop jar mn.jar bayes.NBTestDriver -D delimiter="," -D input="OverSample" -D output="testresult" -D continousVariables="2,3,4,5,6,7,8" -D discreteVariables="1" -D targetVariable="9"  -D numColumns="9" -D modelPath="model" -D targetClasses="negative,positive"

rm  result/oversample.csv
hadoop fs -get testresult/part-00000 result/oversample.csv
