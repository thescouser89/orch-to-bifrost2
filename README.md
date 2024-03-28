# Bunch of scripts to migrate from Orch logs to bifrost

## Generate SQL scripts to extract the logs from pnc-orch into csv files

The csv files are generated for a particular duration (say 1 week) so as not to
create too huge csv files.

```
python query-migrate.py
```
This will print to stdout the sql queries to run. Run them on the Postgres
server

You can customize the script for the start, end time, and for the duration of
the csv files.

```
## Compile the app to load the content of the csv file into bifrost's database

Let's compile the app first
```
mvn clean install -DskipTests=true
```

Then we run it:
```
CSV_FOLDER=/tmp/csvs java -jar target/orch-to-bifrost2-runner.jar
```

This will communicate with the bifrost database (you may have to setup the
quarkus datasource configs) and upload the logs to the finallog.

