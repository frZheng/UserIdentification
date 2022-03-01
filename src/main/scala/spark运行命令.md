RCB: 
spark2-submit --master yarn --class sh_subway ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/SH/SubwayInfo/*  hdfs://compute-5-2:8020/user/zhaojuanjuan/SH/SubwayName

spark2-submit --master yarn --class active2 --num-executors 16 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/  /Destination/subway-pair/*

spark2-submit --master yarn --class MultiSamples_SH --num-executors 16 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class MobilityPatternCount_SH --num-executors 16 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class ODAverageTime_SH --num-executors 16 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class OriginalFeature_SH --num-executors 16 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class SubwayStations_SH --num-executors 16 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class NewFeatures --num-executors 16 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class  SH --num-executors 16 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=12g  --conf spark.default.parallelism=1000 --conf spark.shuffle.memoryFraction=0.8  ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/  SH/Subway/part-00000

spark2-submit --master yarn --class ReadResults ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/zlt/RCB-2021/EmpiricalEstimation_SH

spark2-submit --master yarn --class  RankDistribution --num-executors 32 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class  TripsNumDistribution --num-executors 32 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class ActiveDaysDistribution  --num-executors 32 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class  GroupByOT --num-executors 50 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=4g  --conf spark.default.parallelism=1000  ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/ 0

spark2-submit --master yarn --class EmpiricalEstimation  ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class MarkovChain ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class OriginalFeature  --num-executors 50 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=4g  --conf spark.default.parallelism=1000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/


spark2-submit --master yarn --class ODPeriodicFlow --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=16 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class AverageMiniDistanceBetweenStation ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class SameInMultiOut --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=16 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class  DataForDesPrediction --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=16 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan 20

spark2-submit --master yarn --class  GenerateDataForPrediction --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=16 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class  ODFeatureExtraction --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=16 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/RouteChoiceBehavior.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class  PersonalRouteChoice --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=16 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/RouteChoiceBehavior.jar hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class  RouteChoiceRatioFromAP --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/RouteChoiceBehavior.jar hdfs://compute-5-2:8020/user/zhaojuanjuan


UI:

spark2-submit --master yarn --class LCS --num-executors 64 --conf spark.driver.cores=2 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000 --conf spark.storage.memoryFraction=0.4  --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/ 20

spark2-submit --master yarn --class SOV --num-executors 64 --conf spark.driver.cores=2 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000 --conf spark.storage.memoryFraction=0.4  --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/ 20

spark2-submit --master yarn --class SIG --num-executors 64 --conf spark.driver.cores=2 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000 --conf spark.storage.memoryFraction=0.4  --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/ 20

spark2-submit --master yarn --class AMPIResults ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/zlt/UI-2021/ForLoopResult_arg2

spark2-submit --master yarn --class AMPI_1 --num-executors 64 --conf spark.driver.cores=2 --conf spark.driver.memory=4g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000 --conf spark.storage.memoryFraction=0.4  --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/ 10 4 10

spark2-submit --master yarn --class MostViewPath --num-executors 32 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=4 --conf spark.executor.memory=4g  --conf spark.default.parallelism=1000 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class SegmentsFlowDistribution --num-executors 50 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=4 --conf spark.executor.memory=4g  --conf spark.default.parallelism=1000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class AMPI --num-executors 64 --conf spark.driver.cores=2 --conf spark.driver.memory=4g  --conf spark.executor.cores=4 --conf spark.executor.memory=4g  --conf spark.default.parallelism=2000 --conf spark.storage.memoryFraction=0.4  --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class FlowDistributionOfOverlapSection ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class  OverlapCount --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=16 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class PeakHourODFlow  --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=16 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class FindOverlapAFC   --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=16 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class  AFCAverageTime --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=16 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class  Model --num-executors 100 --conf spark.driver.cores=8 --conf spark.driver.memory=10g  --conf spark.executor.cores=32 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class AFCDataForMatch --num-executors 100 --conf spark.driver.cores=8 --conf spark.driver.memory=10g  --conf spark.executor.cores=32 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer .max=128m  ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class APCompletion --num-executors 100 --conf spark.driver.cores=8 --conf spark.driver.memory=10g  --conf spark.executor.cores=32 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class CollisionStatistics --num-executors 100 --conf spark.driver.cores=8 --conf spark.driver.memory=10g  --conf spark.executor.cores=32 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class SIG --num-executors 100 --conf spark.driver.cores=8 --conf spark.driver.memory=10g  --conf spark.executor.cores=32 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class CountPassengerFlow ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan

spark2-submit --master yarn --class  FilterGroundTruth --num-executors 50 --conf spark.driver.cores=4 --conf spark.driver.memory=4g  --conf spark.executor.cores=4 --conf spark.executor.memory=4g  --conf spark.default.parallelism=2000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/

spark2-submit --master yarn --class test ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/MatchPerMonth/p00028/part-00000  hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/MatchPerMonth/p28

spark2-submit --master yarn --class  MatchPerMonth --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=8g  --conf spark.executor.cores=4 --conf spark.executor.memory=4g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan   hdfs://compute-5-2:8020/user/zhaojuanjuan/Destination/subway-pair/part-0000[3-9]  hdfs://compute-5-2:8020/user/zhaojuanjuan/zlt/UI/NormalMacData/part-*  hdfs://compute-5-2:8020/user/zhaojuanjuan/zlt/UI-2021/MatchResult/p03-09
Top500-part-00003/5/7
Days(15,20]-part-00009/17/25/26/28/32/34
Days(10,15]-part-00011/19/30
Days(20,25]-part-00013/21/27/29/31/33/35/37/39
Days(5,10]-part-00015/23
Days[15,25]-part-00040/41/42/43/44/45/46/47/48/49/50/51/52/53/54/55/56/57

spark2-submit --master yarn --class  ComparisonOfWeekAndMonth --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 --conf  spark.kryoserializer.buffer.max=128m  ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan   hdfs://compute-5-2:8020/user/zhaojuanjuan/Destin/subway-seq/part-00001 hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/NormalMacData/part-*  hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/DataForVerification

0.36028594
Map(0 -> 6891, 1 -> 3881)

spark2-submit --master yarn --class StayTimeStatistic --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/NormalMacData/part-*  hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/StayTimeStatistic

spark2-submit --master yarn --class SamplingAFCData --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/Destin/subway-pair/part*  hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/SampledAFCData

spark2-submit --master yarn --class ODDistribution --num-executors 50 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=4 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  --conf spark.sql.shuffle.partitions=500  --conf spark.shuffle.memoryFraction=0.4 ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/Destin/subway-pair/part* hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/ODDistribution-N

spark2-submit --master yarn --class TimeDistribution --num-executors 50 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=4 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  --conf spark.sql.shuffle.partitions=500  --conf spark.shuffle.memoryFraction=0.4 ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/Destin/subway-seq/part* hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/TimeDistribution


spark2-submit --master yarn --class CountOfMac --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.4 ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/NormalMacData/part-*  hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/DurationStatistic


spark2-submit --master yarn --class NormalMacData --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.4 ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/AllODTimeInterval/ShortPathTime/part-00000  hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/MacData/part* hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/NormalMacData

spark2-submit --master yarn --class AfcAndApContrast --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.4 ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/SampledAFCData/part* hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/AfcAndApContrast_n hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/SampledAPData_n/part*

spark2-submit --master yarn --class SamplingAPData --num-executors 32 --conf spark.driver.cores=2 --conf spark.driver.memory=4g  --conf spark.executor.cores=4 --conf spark.executor.memory=4g  --conf spark.default.parallelism=1000  --conf spark.sql.shuffle.partitions=1000 ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/  0.2

spark2-submit --class ODTimeInterval --master yarn --num-executors 50 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=4 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  --conf spark.sql.shuffle.partitions=500  --conf spark.shuffle.memoryFraction=0.4 ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/AllInfo/stationInfo-UTF-8.txt hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/AllInfo/shortpath.txt  hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UI/AllODTimeInterval hdfs://compute-5-2:8020/user/zhaojuanjuan/Destin/subway-seq/part*

spark2-submit --class FormatSmartCardData --master yarn --num-executors 50 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=4 --conf spark.executor.memory=8g  --conf spark.default.parallelism=1000  --conf spark.sql.shuffle.partitions=500  --conf spark.shuffle.memoryFraction=0.4  ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/Destin/Subway/2019*/part-*  hdfs://compute-5-2:8020/user/zhaojuanjuan/Destin/subway-pair hdfs://compute-5-2:8020/user/zhaojuanjuan/Destin/subway-seq

spark2-submit --master yarn --class MergeAndSort --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=4 --conf spark.executor.memory=10g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/UserIdentification.jar   hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/MacData/part*  hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/MacData-T

spark2-submit --master yarn --class MacCompression --num-executors 100 --conf spark.driver.cores=4 --conf spark.driver.memory=10g  --conf spark.executor.cores=8 --conf spark.executor.memory=8g  --conf spark.default.parallelism=2000  --conf spark.sql.shuffle.partitions=1000  --conf spark.shuffle.memoryFraction=0.5 ~/zlt/UserIdentification.jar  hdfs://compute-5-2:8020/user/zhaojuanjuan/GAD/output/201906/mac_temp/part-*.parquet hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/MacData

spark2-submit --class StatisticDifference --master yarn ~/zlt/SparkProject_1.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/subway_zdbm_station.txt hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/filtered_201608/part-* hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/filtered_201611/part-* hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/results_month

spark2-submit --class LineCount --master yarn ~/zlt/SparkProject_1.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/subway_zdbm_station.txt
totallines:4829421
totallines:8567678
OnlyBus:1984135

spark2-submit --class ColumnFilter --master yarn ~/zlt/SparkProject_1.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/P_GJGD_SZT_201609* hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/filtered_201609

spark2-submit --class FilterObviousChange --executor-memory 8G --master yarn ~/zlt/SparkProject_1.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/subway_zdbm_station.txt hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/filtered_2016*/part-* hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/results/month_08_11

spark2-submit --class ReadParquet --master yarn ~/zlt/SparkProject_1.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/mac/parquetFile/*.parquet hdfs://compute-5-2:8020/user/zhaojuanjuan/Destin/Subway/201906*/* hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/mac-cardId.txt hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/Filtered-Mac hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/Filtered-OD

spark2-submit --class SearchPersonInfo --master yarn ~/zlt/SparkProject_1.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/Destin/Subway/201906*/* hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/mac/part-* 686267562 B8634D799F2D hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/Personal-OD/686267562 hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/Personal-Mac/B8634D799F2D

spark2-submit --class PreProcessing --master yarn ~/zlt/UserIdentification.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/GAD/output/201906/mac_temp/part-*.parquet hdfs://compute-5-2:8020/user/zhaojuanjuan/Destin/Subway/201906*/part-* 687537348 hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/UserIdentify/top_canditates

spark2-submit --class ReadAp_raw --master yarn ~/zlt/SparkProject_1.jar hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/GAD/ap_20181122.csv hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/GAD/ap_raw/20180917/*.parquet hdfs://compute-5-2:8020/user/zhaojuanjuan/liutao/ReadAp_raw/20180917/LaoJie

--driver-memory 8G
–class: 应用程序的入口（例如：org.apache.spark.examples.SparkPi）
–master: 集群的 master URL（如：spark://23.195.26.187:7077）
–deploy-mode: Driver 进程是在集群的 Worker 节点上运行（cluster模式），还是在本地作为一个外部客户端运行（client模式）（默认值是：client）
–conf: 可以设置任意的Spark配置属性，键值对（key=value）格式。如果值中包含空白字符，可以用双引号括起来（”key=value”）

102集群配置
spark2-submit --master yarn\
        --num-executors 50 \
        --conf spark.driver.cores=2 \
        --conf spark.driver.memory=10g \
        --conf spark.driver.maxResultSize=2g \
        --conf spark.executor.cores=2 \
        --conf spark.executor.memory=4g \
        --conf spark.shuffle.blockTransferService=nio \
        --conf spark.memory.fraction=0.8 \
        --conf spark.shuffle.memoryFraction=0.4 \
        --conf spark.default.parallelism=300 \
        --conf spark.sql.shuffle.partitions=400 \
        --conf spark.shuffle.consolidateFiles=true \
        --conf spark.shuffle.io.maxRetries=10 \
        --conf spark.default.parallelism=1000 \
        --conf spark.scheduler.listenerbus.eventqueue.size=1000000 \
        --class cn.sibat.datapd.AppCheckData\
        --name zzzz \
        $sparkApp \
        $jsonPath $clazzPath

HDFS：
hadoop fs -cat /zlt/UI/RankResults/part-* | hadoop fs -put - /zlt/UI/RankResults/part-merged
把HDFS 上的多个文件合并成一个本地文件

hadoop fs -cat /zlt/UI/CountOfMac/part-00000 | tail -n +s | head -n k
查看文件的第s行开始的（包括第s行）前k行数据

hadoop fs -cat filename | wc -l
统计行数

hadoop fs -text /origindata_result/part-merged | zmore
查看文件内容

hadoop fs -rm -r /origindata/2016-04-01-result/_temporary
递归删除文件夹及该文件夹下的文件

hadoop fs -rm -r /origindata/2016-04-01-result/
删除文件夹

查看访问hdfs的正确端口号
hdfs getconf -confKey fs.default.name
>hdfs://IP(host name):Port

查看hdfs内容
hdfs dfs -ls hdfs://IP(host name):Port

查看存储空间
hadoop fs -du -h /user/zhaojuanjuan
hadoop fs -count -q /user/zhaojuanjuan

本地原始数据路径
/parastor/backup/data/SZT/P_GJGD_SZT/
/parastor/backup/data/SZT/SZT_200/201906

102集群数据路径信息：
hadoop fs -ls Destin/Subway
2019/06月份 地铁数据

liutao/carId
已知card ID-mac映射的用户的cardID数据

liutao/mac
已知card ID-mac映射的用户的mac数据

liutao/personalMac或liutao/personalOD
筛选出的某个用户的mac或OD数据

Git：
git删除文件夹
git rm -r --cached .idea #--cached不会把本地的.idea删除
git commit -m 'delete .idea dir'
git push -u origin master