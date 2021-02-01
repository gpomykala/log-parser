package log.parser;

import log.parser.model.LogEventDetails;
import org.slf4j.Logger;

import java.sql.*;

// TODO replace with repository pattern with JPA / Hibernate persistence
public class LogEventWriter {

    private final Logger logger;

    public LogEventWriter(Logger logger){
        this.logger = logger;
    }

    Connection dbConnection;
    long rowsCreated = 0;
    public void init() {
        try {
            try {
                Class.forName("org.hsqldb.jdbcDriver");
            } catch (ClassNotFoundException e) {
                logger.error("Unable to initialize hsqldb driver", e);
            }
            Connection conn = DriverManager.getConnection("jdbc:hsqldb:file:logeventdetailsdb" , "sa", "");
            ensureTableExists(conn);

            try (Statement rowCountStmt = conn.createStatement()) {
                ResultSet rs = rowCountStmt.executeQuery("select count(*) as total from LOGEVENTS");
                while (rs.next()){
                    logger.info(rs.getInt("total") + " rows found in LOGEVENTS table");
                }
            }

            dbConnection = conn;

        }catch (SQLException e){
            logger.error("Unable to initialize HSQLDB connection", e);
            e.printStackTrace();
        }
    }

    public void cleanup() {

        doFlush();

        if (dbConnection != null) {
            try {
                dbConnection.close();
            } catch (SQLException throwables) {
                logger.error("Unable to close HSQL connection", throwables);
                throwables.printStackTrace();
            }
        }
    }

    void ensureTableExists(Connection conn){
        try {
            DatabaseMetaData dbm = conn.getMetaData();
            try(ResultSet rs = dbm.getTables(null, "LOGPARSERAPP", "LOGEVENTS", null)){
                if (!rs.next()) {
                    String createTableStatement = "create table LOGEVENTS(" +
                            "ID varchar(255) NOT NULL, " +
                            "DURATION int, " +
                            "TYPE varchar(255), " +
                            "HOST varchar(255), " +
                            "ALERT bit, " +
                            "primary key (id));";
                    try (PreparedStatement create = conn.prepareStatement(createTableStatement)){
                        create.executeUpdate();
                        logger.info("LOGEVENTS table created");
                    }
                }else{
                    logger.info("LOGEVENTS table already exists");
                }
            }

        } catch (SQLException throwables) {
            logger.error("Unable to create LOGEVENTS table");
            throwables.printStackTrace();
        }
    }

    Statement currentInsertStmt;
    int currentBatchLength = 0;

    // TODO pump and drain events using producer - consumer pattern
    public void write(LogEventDetails eventDetails){
        if(dbConnection == null) {
            logger.error("HSQL connection not found, event will not be persisted");
            return;
        }
        if (eventDetails == null) {
            return;
        }
        try {
            if (currentInsertStmt == null) currentInsertStmt = dbConnection.createStatement();
            String sql = "INSERT INTO LOGEVENTS " +
                    "VALUES (" +
                    "'" +eventDetails.id + "', " +
                    eventDetails.duration + ", " +
                    "'" + eventDetails.type + "', " +
                    "'" + eventDetails.host + "', " +
                    (eventDetails.alert ? 1 : 0)  + ");";
            currentInsertStmt.addBatch(sql);
            currentBatchLength++;

            flush();

            //System.out.println(eventDetails.id + " persisted");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public  long getInsertCount(){
        return rowsCreated;
    }

    private void flush() {
        if(currentBatchLength > 999) {
            doFlush();
        }
    }

    private void doFlush() {
        try {
            if (currentInsertStmt == null) return;
            currentInsertStmt.executeBatch();
            rowsCreated += currentBatchLength;
            currentInsertStmt.close();
            currentInsertStmt = null;
            currentBatchLength = 0;
        } catch (SQLException throwables) {
            logger.error("Batch insert failed", throwables);
            throwables.printStackTrace();
        }
    }
}
