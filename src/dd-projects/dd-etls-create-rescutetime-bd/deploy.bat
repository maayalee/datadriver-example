mvn compile exec:java ^
-Dexec.mainClass=com.maayalee.dd.etls.createrescuetimebd.StarterPipeline ^
-Dexec.args="--runner=DataflowRunner --project=fast-archive-274910 --region=asia-northeast1-c --templateLocation=gs://datadriver-dataflow-fast-archive-274910/templates/dd-etls-create-rescuetime --clusteringField=user_id --tableSchemaJSONPath=gs://datadriver-dataflow-fast-archive-274910/schemas/rescuetime_tbl_bd_data.json"