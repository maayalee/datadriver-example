java -jar dd-importers-load-googlefitness.jar ^
-begin_time=2020-04-29T00:00:00.00+09:00 ^
-end_time=2020-04-29T23:59:59.99+09:00 ^
-output_directory=gs://datadriver-datalake-fast-archive-274910/data/log/googlefitness ^
-output_filenameprefix=20200429+9:00- ^
-shard_size=1
