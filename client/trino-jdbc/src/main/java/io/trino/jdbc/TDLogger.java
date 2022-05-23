package io.trino.jdbc;

import com.treasure_data.logger.TreasureDataLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.treasure_data.logger.Constants.TD_LOGGER_AGENTMODE;

public class TDLogger
{
    static TDLogger logger = new NullLogger();

    static synchronized void initLogger(String tdApiKey)
    {
        if (logger == null || logger instanceof NullLogger) {
            logger = new TDLogger(tdApiKey);
        }
    }

    private final TreasureDataLogger tdLogger;

    TDLogger(String tdApiKey)
    {
        if (tdApiKey != null) {
            Properties properties = new Properties();
            properties.put("td.logger.api.key", tdApiKey);
            properties.put("td.logger.api.server.host", "api-development-import.treasuredata.com");
            properties.put("td.logger.api.server.port", "443");
            properties.put("td.logger.api.server.schema", "https");
            properties.put("td.logger.create.table.auto", "true");
            properties.put(TD_LOGGER_AGENTMODE, "false");
            tdLogger = TreasureDataLogger.getLogger("mitsudb", properties);
        }
        else {
            tdLogger = null;
        }
    }

    public void logMethodCall(String clazz, String method)
    {
        logMethodCall(clazz, method, null);
    }

    public void logMethodCall(String clazz, String method, String value)
    {
        Map<String, Object> map = new HashMap<>();
        map.put("clazz", clazz);
        map.put("method", method);
        if (value != null) {
            map.put("value", value);
        }
        tdLogger.log("sisense_jdbc_log", map);
    }

    public void close()
    {
        tdLogger.close();
    }

    static class NullLogger
        extends TDLogger
    {
        public NullLogger()
        {
            super(null);
        }

        @Override
        public void logMethodCall(String clazz, String method)
        {
        }

        @Override
        public void logMethodCall(String clazz, String method, String value)
        {
        }

        @Override
        public void close()
        {
        }
    }
}

