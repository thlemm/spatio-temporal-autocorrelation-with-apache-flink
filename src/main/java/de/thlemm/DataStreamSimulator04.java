package de.thlemm;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Date;
import com.mysql.cj.jdbc.MysqlDataSource;
import de.thlemm.records.TripEvent;
import de.thlemm.records.TripEventSerializationSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;


public class DataStreamSimulator04
{
    public static String server = "localhost";
    public static int port = 3306;
    public static String databaseName = "tlc_trip_record_data";
    public static String username = "streamer";
    public static String password = "";

    public static void main(String[] args) {

        String topic = "input_sample_data";

        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps);

        try {
            Connection con = createConnection();
            System.out.println("Connection established");

            assert con != null;
            streamTable(con, databaseName, topic, producer);
            con.close();

        }
        catch(SQLException ex) {
            System.out.println(ex);
        }

    } // end main

    public static Connection createConnection(){
        try {
            MysqlDataSource dataSource = new MysqlDataSource();
            System.out.println("MysqlDataSource implements the javax.sql.DataSource interface");

            System.out.println("datasource created");
            dataSource.setServerName(server);
            dataSource.setPort(port);
            dataSource.setDatabaseName(databaseName);
            dataSource.setUser(username);
            dataSource.setPassword(password);

            return dataSource.getConnection();

        }
        catch(SQLException ex){
            System.out.println(ex);
            return null;
        }
    }

    public static void streamTable(Connection con, String dbName, String topic, KafkaProducer<byte[], byte[]> producer)
            throws SQLException {

        Statement stmt = null;

        String query =
                "select tpep_pickup_datetime, pulocationid " +
                        "from " + dbName + ".2018_01_yellow_taxi_trip_data_min" +
                        // " where tpep_pickup_datetime >= '2018-01-09 14:00:00' and tpep_pickup_datetime <= '2018-01-09 17:00:00'" +
                        // " where tpep_pickup_datetime >= '2018-01-01 00:00:00' and tpep_pickup_datetime <= '2018-01-31 00:00:00'" +
                        // " where tpep_pickup_datetime >= '2018-01-25 18:00:00' and tpep_pickup_datetime <= '2018-01-27 20:00:00'" +
                        " where tpep_pickup_datetime >= '2018-01-05 18:00:00' and tpep_pickup_datetime <= '2018-01-07 20:00:00'" +
                        // " and (pulocationid >= 0 and pulocationid <= 10)" +
                        " order by tpep_pickup_datetime" +
                        ";";


/*        String query =
                "select tpep_dropoff_datetime, dolocationid " +
                        "from " + dbName + ".2018_01_yellow_taxi_trip_data_min" +
                        // " where tpep_pickup_datetime >= '2018-01-09 14:00:00' and tpep_pickup_datetime <= '2018-01-09 17:00:00'" +
                        // " where tpep_dropoff_datetime >= '2017-12-31 18:00:00' and tpep_dropoff_datetime <= '2018-02-01 00:00:00'" +
                        " where tpep_dropoff_datetime >= '2018-01-25 18:00:00' and tpep_dropoff_datetime <= '2018-01-27 20:00:00'" +
                        // " where tpep_dropoff_datetime >= '2018-01-05 18:00:00' and tpep_dropoff_datetime <= '2018-01-07 20:00:00'" +
                        // " and (pulocationid >= 0 and pulocationid <= 10)" +
                        " order by tpep_dropoff_datetime" +
                        ";";*/


        /*
        String query =
                "select tpep_pickup_datetime, pulocationid " +
                        "from " + dbName + ".2018_01_yellow_taxi_trip_data_min" +
                        " where tpep_pickup_datetime >= '2018-01-08 23:00:00' and tpep_pickup_datetime <= '2018-01-10 06:00:00'" +
                         " and (pulocationid >= 0 and pulocationid <= 10)" +
                        " order by tpep_pickup_datetime" +
                        ";";

         */

        System.out.println(query);

        try {
            stmt = con.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                // Date tpep_datetime = rs.getTimestamp("tpep_dropoff_datetime");
                // String locationid = rs.getString("dolocationid");
                Date tpep_datetime = rs.getTimestamp("tpep_pickup_datetime");
                String locationid = rs.getString("pulocationid");
                // System.out.println(tpep_datetime + "\t" + locationid);


                ProducerRecord<byte[], byte[]> record = new TripEventSerializationSchema(topic)
                        .serialize(new TripEvent(tpep_datetime, locationid),
                                null);

                producer.send(record);


            }
        } catch (SQLException ex ) {
            System.out.println(ex);
        } finally {
            if (stmt != null) { stmt.close(); }
            if (con != null) {con.close(); }
            System.out.println("The connection was closed.");
        }
    }

}  // end class