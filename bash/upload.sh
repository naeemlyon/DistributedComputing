clear

sudo mv /home/mnaeem/workspace/data/sampleFolder/marginalization.sh marginalization.sh

hadoop fs -rmr sampleFolder

hadoop fs -put /home/mnaeem/workspace/data/sampleFolder

hadoop fs -ls sampleFolder

bash marginalization.sh



