java -jar dd-importers-load-rescuetime.jar \
-user_id=maayalee \
-api_key=B63lJwmLgMWhDcvVf9nmVUtwtVVagWPrZmgFiBF9 \
-input_begin_date=2020-05-16 \
-input_end_date=2020-05-17 \
-input_timezone=Asia/Seoul \
-output_date=2020-05-16 \
-output_timezone=UTC \
-output_directory=gs://datadriver-datalake-fast-archive-274910/data/log/rescuetime \
-output_filenameprefix=20200516Z-maayalee- \
-shard_size=3
