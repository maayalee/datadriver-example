java -jar dd-importers-load-googlefitness.jar ^
-user_id=maayalee ^
-begin_time=2020-06-16T00:00:00.00Z ^
-end_time=2020-06-16T23:59:59.99Z ^
-output_directory=gs://datadriver-datalake-fast-archive-274910/data/log/googlefitness ^
-output_filenameprefix=20200616Z-maayalee- ^
-shard_size=3
