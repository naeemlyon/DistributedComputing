clear

###############################################

hadoop jar mn.jar underSamplingNBTrain.Driver -D delimiter="," -D inputFolder="sampleFolder" -D output="model" -D samples="2" -D continousVariables="1,2,3,4" -D discreteVariables="" -D targetVariable="5"  -D numColumns="9"  

hadoop fs -ls model
rm -rf result/models
hadoop fs -get model result/models
###############################################

hadoop jar mn.jar underSamplingNBTest.Driver -D delimiter="," -D input="iris.data" -D modelCount="2" -D output="testresult" -D continousVariables="1,2,3,4" -D discreteVariables="" -D targetVariable="5" -D numColumns="9"  -D modelPath="model" -D targetClasses="Iris-virginica,Iris-setosa,Iris-versicolor" -D evaluation="true"  

rm result/testResult
hadoop fs -get testresult/part-00000 result/testResult
###############################################

