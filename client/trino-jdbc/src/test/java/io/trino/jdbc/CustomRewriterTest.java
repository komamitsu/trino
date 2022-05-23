package io.trino.jdbc;

import org.junit.AfterClass;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class CustomRewriterTest
{
    private static final String DB_URL = "jdbc:trino://api-development-presto.treasuredata.com:443/td-presto";

    private static final String SQL_YEAR_AGGREGATION =
            "WITH tq_gnOjk2mbD_cTable AS (\n" +
                    "  SELECT *, from_unixtime(\"time\") as t\n" +
                    "  FROM \"sample_datasets\".\"www_access\"\n" +
                    ")\n" +
                    "SELECT [YsnsAls_0002] FROM (\n" +
                    "  SELECT\n" +
                    "    ROW_NUMBER() OVER (ORDER BY (SELECT [TempTableQuerySchema].[YsnsAls_0002]) ASC) as rn,\n" +
                    "    (CAST(DateAdd(yy,YEAR([TempTableQuerySchema].[YsnsAls_0002]) - 1904, DateAdd(mm,1 - 1, DateAdd(dd, 1 - 1, '1904-01-01'))) AS DateTime)) AS [YsnsAls_0002]\n" +
                    "  FROM (\n" +
                    "    SELECT ([TempTableQuerySchema].[YsnsAls_0002]) AS [YsnsAls_0002]\n" +
                    "    FROM (\n" +
                    "      SELECT ([TempTableQuerySchema].[YsnsAls_0002]) AS [YsnsAls_0002]\n" +
                    "      FROM (\n" +
                    "        SELECT ([TempTableQuerySchema].[YsnsAls_0002]) AS [YsnsAls_0002] FROM (\n" +
                    "          SELECT (CAST(DateAdd(yy,YEAR([tq_gnOjk2mbD].[t]) - 1904, DateAdd(mm,1 - 1, DateAdd(dd, 1 - 1, '1904-01-01'))) AS DateTime)) AS [YsnsAls_0002]\n" +
                    "          FROM [tq_gnOjk2mbD_cTable] [tq_gnOjk2mbD]\n" +
                    "        ) AS [TempTableQuerySchema]\n" +
                    "      ) AS [TempTableQuerySchema]\n" +
                    "      GROUP BY [TempTableQuerySchema].[YsnsAls_0002]\n" +
                    "    ) AS [TempTableQuerySchema]\n" +
                    "  ) AS [TempTableQuerySchema]\n" +
                    ") AS [TempTableQuerySchema]\n" +
                    "WHERE (rn > 0 AND rn <= 50001 )\n" +
                    "ORDER BY [TempTableQuerySchema].[YsnsAls_0002] ASC";

    private static final String SQL_DAY_AGGREGATION =
            "WITH tq_gH9v2OuNw_cTable AS (\n" +
                    "  SELECT *, from_unixtime(\"time\") as t FROM \"sample_datasets\".\"www_access\"\n" +
                    ")\n" +
                    "SELECT [DsnsAls_0014], [snsCol_0002], [snsCol_0004]\n" +
                    "FROM (\n" +
                    "  SELECT\n" +
                    "    ROW_NUMBER() OVER (ORDER BY (SELECT [snsTbl__0001].[DsnsAls_0014]) ASC) as rn,\n" +
                    "    (CAST(\n" +
                    "      DateAdd(yy,\n" +
                    "        YEAR([snsTbl__0001].[DsnsAls_0014]) - 1904,\n" +
                    "          DateAdd(mm,\n" +
                    "            MONTH([snsTbl__0001].[DsnsAls_0014]) - 1,\n" +
                    "            DateAdd(dd,\n" +
                    "              DAY([snsTbl__0001].[DsnsAls_0014]) - 1,\n" +
                    "              '1904-01-01'))) AS DateTime)) AS [DsnsAls_0014],\n" +
                    "    ([snsTbl__0001].[snsCol_0002]) AS [snsCol_0002],\n" +
                    "    ([snsTbl__0001].[snsCol_0004]) AS [snsCol_0004]\n" +
                    "FROM (\n" +
                    "  SELECT\n" +
                    "    ([snsTbl__0001].[DsnsAls_0014]) AS [DsnsAls_0014],\n" +
                    "    ([snsTbl__0001].[snsCol_0002]) AS [snsCol_0002],\n" +
                    "    ([snsTbl__0001].[snsCol_0003]) AS [snsCol_0004]\n" +
                    "  FROM (\n" +
                    "    SELECT\n" +
                    "      ([snsTbl__0001].[DsnsAls_0014]) AS [DsnsAls_0014],\n" +
                    "      ([snsTbl__0001].[snsCol_0002]) AS [snsCol_0002],\n" +
                    "      ([snsTbl__0001].[snsCol_0003]) AS [snsCol_0003]\n" +
                    "    FROM (\n" +
                    "      SELECT\n" +
                    "        ([TempTableQuerySchema].[DsnsAls_0014]) AS [DsnsAls_0014],\n" +
                    "        ([TempTableQuerySchema].[snsCol_0002]) AS [snsCol_0002],\n" +
                    "        (COUNT([TempTableQuerySchema].[snsCol_0003])) AS [snsCol_0003]\n" +
                    "      FROM (\n" +
                    "        SELECT\n" +
                    "          ([TempTableQuerySchema].[DsnsAls_0014]) AS [DsnsAls_0014],\n" +
                    "          ([TempTableQuerySchema].[snsCol_0002]) AS [snsCol_0002],\n" +
                    "          ([TempTableQuerySchema].[snsCol_0003]) AS [snsCol_0003]\n" +
                    "        FROM (\n" +
                    "          SELECT\n" +
                    "            ([tq_gH9v2OuNw].[snsCol_0003]) AS [snsCol_0003],\n" +
                    "            ([tq_gH9v2OuNw].[DsnsAls_0014]) AS [DsnsAls_0014],\n" +
                    "            ([tq_gH9v2OuNw].[snsCol_0002]) AS [snsCol_0002]\n" +
                    "          FROM (\n" +
                    "            SELECT\n" +
                    "              ([tq_gH9v2OuNw].[code]) AS [snsCol_0003],\n" +
                    "              (CAST(DateAdd(yy,YEAR([tq_gH9v2OuNw].[t]) - 1904, DateAdd(mm,MONTH([tq_gH9v2OuNw].[t]) - 1, DateAdd(dd, DAY([tq_gH9v2OuNw].[t]) - 1, '1904-01-01'))) AS DateTime)) AS [DsnsAls_0014],\n" +
                    "              ([tq_gH9v2OuNw].[code]) AS [snsCol_0002]\n" +
                    "            FROM [tq_gH9v2OuNw_cTable] [tq_gH9v2OuNw]\n" +
                    "          ) AS [tq_gH9v2OuNw]\n" +
                    "        ) AS [TempTableQuerySchema]\n" +
                    "      ) AS [TempTableQuerySchema]\n" +
                    "      GROUP BY\n" +
                    "        [TempTableQuerySchema].[DsnsAls_0014],\n" +
                    "        [TempTableQuerySchema].[snsCol_0002]\n" +
                    "      ) AS [snsTbl__0001]\n" +
                    "    ) AS [snsTbl__0001]\n" +
                    "  ) AS [snsTbl__0001]\n" +
                    ") AS [snsTbl__0001]\n" +
                    "WHERE (rn > 0 AND rn <= 50001 )\n" +
                    "ORDER BY [snsTbl__0001].[DsnsAls_0014] ASC";

    private static final String SQL_HOUR_AGGREGATION =
            /*
            "WITH tq_gH9v2OuNw_cTable AS (\n" +
                    "  SELECT *, from_unixtime(\"time\") as t FROM \"sample_datasets\".\"www_access\"\n" +
                    ")\n" +
                    "SELECT [snsCol_0001] FROM (\n" +
                    "  SELECT ROW_NUMBER() OVER (ORDER BY (SELECT [TempTableQuerySchema].[snsCol_0001]) ASC) as rn,\n" +
                    "  (CONCAT('1111-11-11T',CAST([TempTableQuerySchema].[snsCol_0001] AS TIME))) AS [snsCol_0001] FROM (\n" +
                    "    SELECT ([TempTableQuerySchema].[snsCol_0001]) AS [snsCol_0001]\n" +
                    "    FROM (SELECT ([TempTableQuerySchema].[snsCol_0001]) AS [snsCol_0001] FROM (\n" +
                    "    SELECT (\n" +
                    "      dateadd(s,\n" +
                    "        (DATEDIFF(SECOND, 0,cast ([TempTableQuerySchema].[snsCol_0001] as  time))\n" +
                    "        - DATEDIFF(SECOND, 0,cast ([TempTableQuerySchema].[snsCol_0001] as  time))\n" +
                    "        % (CAST('3600000' AS INT)/1000)), 0 )) AS [snsCol_0001]\n" +
                    "    FROM (\n" +
                    "      SELECT (\n" +
                    "        dateadd(s,\n" +
                    "          (DATEDIFF(SECOND, 0,cast ([tq_gH9v2OuNw].[t] as  time))\n" +
                    "          - DATEDIFF(SECOND, 0,cast ([tq_gH9v2OuNw].[t] as  time))\n" +
                    "          % (CAST('3600000' AS INT)/1000)), 0 )) AS [snsCol_0001]\n" +
                    "      FROM [tq_gH9v2OuNw_cTable] [tq_gH9v2OuNw]\n" +
                    "    ) AS [TempTableQuerySchema]\n" +
                    "  ) AS [TempTableQuerySchema]\n" +
                    "  GROUP BY [TempTableQuerySchema].[snsCol_0001]\n" +
                    ") AS [TempTableQuerySchema]\n" +
                    ") AS [TempTableQuerySchema]\n" +
                    ") AS [TempTableQuerySchema\n" +
                    "WHERE (rn > 0 AND rn <= 50001\n" +
                    "ORDER BY [TempTableQuerySchema].[snsCol_0001] ASC";
             */
            "WITH tq_gH9v2OuNw_cTable AS (\n" +
            "  SELECT *, from_unixtime(\"time\") as t FROM \"sample_datasets\".\"www_access\"\n" +
            ")\n" +
            "SELECT [snsCol_0001] FROM (\n" +
            "  SELECT ROW_NUMBER() OVER (ORDER BY (SELECT [TempTableQuerySchema].[snsCol_0001]) ASC) as rn,\n" +
            "  (CONCAT('1111-11-11T',CAST([TempTableQuerySchema].[snsCol_0001] AS TIME))) AS [snsCol_0001] FROM (\n" +
            "    SELECT ([TempTableQuerySchema].[snsCol_0001]) AS [snsCol_0001]\n" +
            "    FROM (SELECT ([TempTableQuerySchema].[snsCol_0001]) AS [snsCol_0001] FROM (\n" +
            "    SELECT (\n" +
            "      dateadd(s,\n" +
            "        (DATEDIFF(SECOND, 0,cast ([TempTableQuerySchema].[snsCol_0001] as  time))\n" +
            "        - DATEDIFF(SECOND, 0,cast ([TempTableQuerySchema].[snsCol_0001] as  time))\n" +
            "        % (CAST('3600000' AS INT)/1000)), 0 )) AS [snsCol_0001]\n" +
            "    FROM (\n" +
            "      SELECT (\n" +
            "        dateadd(s,\n" +
            "          (DATEDIFF(SECOND, 0,cast ([tq_gH9v2OuNw].[t] as  time))\n" +
            "          - DATEDIFF(SECOND, 0,cast ([tq_gH9v2OuNw].[t] as  time))\n" +
            "          % (CAST('3600000' AS INT)/1000)), 0 )) AS [snsCol_0001]\n" +
            "      FROM [tq_gH9v2OuNw_cTable] [tq_gH9v2OuNw]\n" +
            "    ) AS [TempTableQuerySchema]\n" +
            "  ) AS [TempTableQuerySchema]\n" +
            "  GROUP BY [TempTableQuerySchema].[snsCol_0001]\n" +
            ") AS [TempTableQuerySchema]\n" +
            ") AS [TempTableQuerySchema]\n" +
            ") AS [TempTableQuerySchema] WHERE (rn > 0 AND rn <= 50001 ) ORDER BY [TempTableQuerySchema].[snsCol_0001] ASC";

    private static final String SQL_QUARTER_AGGREGATION =
            "WITH tq_gH9v2OuNw_cTable AS (\n" +
                    "  SELECT *, from_unixtime(\"time\") as t FROM \"sample_datasets\".\"www_access\"\n" +
                    ")\n" +
                    "SELECT [QsnsAls_0007], [snsCol_0003] FROM (\n" +
                    "  SELECT ROW_NUMBER() OVER (ORDER BY (SELECT [snsTbl__0001].[QsnsAls_0007]) ASC) as rn,\n" +
                    "  (CAST(DateAdd(yy,YEAR([snsTbl__0001].[QsnsAls_0007]) - 1904, DateAdd(mm,((((3) * (DATEPART(\"q\",[snsTbl__0001].[QsnsAls_0007])))) - 2) - 1, DateAdd(dd, 1 - 1, '1904-01-01'))) AS DateTime)) AS [QsnsAls_0007],\n" +
                    "  ([snsTbl__0001].[snsCol_0003]) AS [snsCol_0003]\n" +
                    "  FROM (\n" +
                    "    SELECT\n" +
                    "      ([snsTbl__0001].[QsnsAls_0007]) AS [QsnsAls_0007],\n" +
                    "      ([snsTbl__0001].[snsCol_0002]) AS [snsCol_0003]\n" +
                    "    FROM (\n" +
                    "      SELECT\n" +
                    "        ([snsTbl__0001].[QsnsAls_0007]) AS [QsnsAls_0007],\n" +
                    "        ([snsTbl__0001].[snsCol_0002]) AS [snsCol_0002]\n" +
                    "      FROM (\n" +
                    "        SELECT\n" +
                    "          ([TempTableQuerySchema].[QsnsAls_0007]) AS [QsnsAls_0007],\n" +
                    "          (COUNT([TempTableQuerySchema].[snsCol_0002])) AS [snsCol_0002]\n" +
                    "        FROM (\n" +
                    "          SELECT\n" +
                    "            ([TempTableQuerySchema].[QsnsAls_0007]) AS [QsnsAls_0007],\n" +
                    "            ([TempTableQuerySchema].[snsCol_0002]) AS [snsCol_0002]\n" +
                    "          FROM (\n" +
                    "            SELECT\n" +
                    "              ([tq_gH9v2OuNw].[snsCol_0002]) AS [snsCol_0002],\n" +
                    "              ([tq_gH9v2OuNw].[QsnsAls_0007]) AS [QsnsAls_0007]\n" +
                    "            FROM (\n" +
                    "              SELECT\n" +
                    "                ([tq_gH9v2OuNw].[code]) AS [snsCol_0002],\n" +
                    "                (CAST(DateAdd(yy,YEAR([tq_gH9v2OuNw].[t]) - 1904, DateAdd(mm,((((3) * (DATEPART(\"q\",[tq_gH9v2OuNw].[t])))) - 2) - 1, DateAdd(dd, 1 - 1, '1904-01-01'))) AS DateTime)) AS [QsnsAls_0007]\n" +
                    "              FROM [tq_gH9v2OuNw_cTable] [tq_gH9v2OuNw]\n" +
                    "            ) AS [tq_gH9v2OuNw]\n" +
                    "          ) AS [TempTableQuerySchema]\n" +
                    "        ) AS [TempTableQuerySchema]\n" +
                    "        GROUP BY [TempTableQuerySchema].[QsnsAls_0007]\n" +
                    "      ) AS [snsTbl__0001]\n" +
                    "    ) AS [snsTbl__0001]\n" +
                    "  ) AS [snsTbl__0001]\n" +
                    ") AS [snsTbl__0001]\n" +
                    "WHERE (rn > 0 AND rn <= 50001 )\n" +
                    "ORDER BY [snsTbl__0001].[QsnsAls_0007] ASC";

    @AfterClass
    public static void tearDown()
    {
        TDLogger.logger.close();
    }

    private void rewriteAndAssertSQL(String sql, List<String> expected)
        throws Throwable
    {
        String tdApikey = System.getenv("TD_APIKEY");
        if (tdApikey == null) {
            System.err.println("TD_APIKEY isn't set. Skipping this test");
            return;
        }
        Connection conn = null;
        try {
            DriverManager.registerDriver(new TrinoDriver());
            conn = DriverManager.getConnection(DB_URL, tdApikey, "");
            CallableStatement stmt = conn.prepareCall(sql);
            if (!stmt.execute()) {
                throw new RuntimeException("Should return resultset");
            }
            ResultSet rs = stmt.getResultSet();
            List<String> actual = new ArrayList<>();
            // Extract data from result set
            while (rs.next()) {
                actual.add(rs.getString(1));
            }

            assertEquals(
                    expected.stream().sorted().collect(Collectors.toList()),
                    actual.stream().sorted().collect(Collectors.toList()));
        }
        catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
        finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void rewriteInvalidYearAggregationSQL()
            throws Throwable
    {
        List<String> expected = Arrays.asList("2014-01-01 00:00:00.000");
        rewriteAndAssertSQL(SQL_YEAR_AGGREGATION, expected);
    }

    @Test
    public void rewriteInvalidDayAggregationSQL()
            throws Throwable
    {
        List<String> expected = Arrays.asList(
                "2014-10-03 00:00:00.000",
                "2014-10-03 00:00:00.000",
                "2014-10-04 00:00:00.000",
                "2014-10-04 00:00:00.000",
                "2014-10-04 00:00:00.000"
        );
        rewriteAndAssertSQL(SQL_DAY_AGGREGATION, expected);
    }

    @Test
    public void rewriteInvalidHourAggregationSQL()
            throws Throwable
    {
        List<String> expected = Arrays.asList(
                "1111-11-11T00:00:00",
                "1111-11-11T01:00:00",
                "1111-11-11T07:00:00",
                "1111-11-11T08:00:00",
                "1111-11-11T09:00:00",
                "1111-11-11T10:00:00",
                "1111-11-11T11:00:00",
                "1111-11-11T12:00:00",
                "1111-11-11T13:00:00",
                "1111-11-11T14:00:00",
                "1111-11-11T15:00:00",
                "1111-11-11T16:00:00",
                "1111-11-11T17:00:00",
                "1111-11-11T18:00:00",
                "1111-11-11T19:00:00",
                "1111-11-11T20:00:00",
                "1111-11-11T21:00:00",
                "1111-11-11T22:00:00",
                "1111-11-11T23:00:00"
        );
        rewriteAndAssertSQL(SQL_HOUR_AGGREGATION, expected);
    }
}
