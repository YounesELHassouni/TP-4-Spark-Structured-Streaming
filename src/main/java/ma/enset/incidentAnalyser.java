package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class incidentAnalyser {
    private static final String APP_NAME = "Incidents Analyser";
    private static final String SPARK_MASTER = "spark://spark-master:7077";
    private static final String FILES_LOCATION = "hdfs://namenode:8020/incidents";
    private static final String LOG_LEVEL = "WARN";

    static SparkSession getSparkSession(){
        return SparkSession.builder()
                .appName(APP_NAME)
                .master(SPARK_MASTER)
                .getOrCreate();
    }

    public static StructType getSchema(){
        return new StructType(
                new StructField[]{
                        new StructField("Id", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("Title", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("Description", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("Service", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("Date", DataTypes.DateType, false, Metadata.empty())
                }
        );
    }

    public static void main(String[] args) {
        try(SparkSession sparkSession = getSparkSession()){
            sparkSession.sparkContext().setLogLevel(LOG_LEVEL);
            Dataset<Row> incidents = sparkSession.readStream()
                    .schema(getSchema())
                    .option("header", "true")
                    .csv(FILES_LOCATION);

            StreamingQuery numIncidentsByService = incidents.groupBy("Service")
                    .agg(count("*").alias("Number_of_Incidents"))
                    .writeStream()
                    .outputMode(OutputMode.Complete())
                    .format("console")
                    .start();
            StreamingQuery mostIncidentsTwoYear = incidents
                    .groupBy(year(col("Date")).alias("Year_incidents"))
                    .agg(count("*").alias("Number_of_Incidents"))
                    .orderBy(col("Number_of_Incidents").desc())
                    .limit(2)
                    .writeStream()
                    .outputMode(OutputMode.Complete())
                    .format("console")
                    .start();

            numIncidentsByService.awaitTermination();
            mostIncidentsTwoYear.awaitTermination();



        } catch (TimeoutException | StreamingQueryException e) {
            throw new RuntimeException(e);
        }
    }

}
