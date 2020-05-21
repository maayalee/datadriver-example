java -jar dd-etls-create-json-bd.jar ^
--jobName=create-json-bd-20200519 ^
--runner=DataflowRunner ^
--project=fast-archive-274910 ^
--tempLocation=gs://datadriver-dataflow-fast-archive-274910/tmp ^
--stagingLocation=gs://datadriver-dataflow-fast-archive-274910/tmp ^
--inputFilePattern=gs://datadriver-datalake-fast-archive-274910/data/log/googlefitness/20200519+9:00-datasets-* ^
--timezone=Asia/Seoul ^
--tableSchemaJSONPath=gs://datadriver-dataflow-fast-archive-274910/schemas/googlefitness_tbl_bd_datasets.json ^
--outputTable=fast-archive-274910:dw_datadriver.googlefitness_tbl_bd_datasets$20020519
