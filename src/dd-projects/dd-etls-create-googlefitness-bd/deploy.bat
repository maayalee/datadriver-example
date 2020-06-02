mvn compile exec:java ^
-Dexec.mainClass=com.maayalee.dd.etls.creategooglefitnessbd.StarterPipeline ^
-Dexec.args="--runner=DataflowRunner --project=rising-field-212511 --stagingLocation=gs://datadriver-dataflow/binaries --templateLocation=gs://datadriver-dataflow/templates/dd-analyzers-create-json-bd --tempLocation=gs://datadriver-dataflow/temp --zone=asia-northeast1-c"
