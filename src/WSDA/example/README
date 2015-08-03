Instructions to run on sample data set:
ap_sequence is a sequence file of the dataset. Here we assume that it is located at the root of HDFS. 


1-Vectorize data set

spark/bin/spark-submit --class VectorizeCorpus LDA.jar /ap_sequence.dat False 4 /vocab /vsm

2-Run LDA

spark/bin/spark-submit --class LDA LDA.jar /vsm/* 22000 10 /results example_settings

3-Get result 
python topics.py /results/lambda_final /vocab/part-00000 20 


