clear
#hadoop jar mn.jar reducejoin.PrepareBashSingleKey
#rm -r out
##################################################################################
#hadoop jar mn.jar reducejoin.JoinDriver -D input="test train" -D output="out" -D joinType="inner" -D KeysA="7" -D ValA="0,1,2,3,4,5,6" -D DLMT="," -D 
#KeysB="6" -D ValB="0,1,2,3,4,5,7"
#hadoop fs -get out out
##################################################################################
#generate the scripts
#hadoop jar mn.jar reducejoin.PrepareBashSingleKey
###################################################################################
# Aggregation
hadoop jar mn.jar aggregation.Aggregate -D input="/aggr" -D output="out" -D Key="8" -D ValNum="3,4,6,7" -D ValDate"2" -D ValText="0,1,5" -D ValOther="" -D DLMT="," > TraceLog
rm aggregated.csv
hadoop fs -get out/part-r-00000 aggregated.csv
###################################################################################

