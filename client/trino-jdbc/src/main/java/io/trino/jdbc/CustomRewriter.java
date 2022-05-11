package io.trino.jdbc;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.PrestoSqlDialect;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.util.TimestampString;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CustomRewriter
{
    private final Pattern PATTERN_PREPARE = Pattern.compile("((?:PREPARE|prepare)\\s+\\w+\\s+(?:FROM|from)\\s+)(.*)", Pattern.DOTALL);

    private final Pattern PATTERN_EXECUTE = Pattern.compile("((?:EXECUTE|execute)\\s+)(.*)", Pattern.DOTALL);

    private static class SqlRewriter
            extends SqlValidatorImpl
    {
        SqlRewriter(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, Config config)
        {
            super(opTab, catalogReader, typeFactory, config);
        }

        SqlNode rewrite(SqlNode node)
        {
            return super.performUnconditionalRewrites(node, false);
        }
    }

    private static class SqlVisitor
            extends SqlShuttle
    {
        private SqlNode parseTimeStampValue(SqlNode node, SqlParserPos pos)
        {
            SqlNode newNode;
            if (node.getKind() == SqlKind.LITERAL) {
                String s = node.toString();
                try {
                    newNode = SqlTimestampLiteral.createTimestamp(
                            TimestampString.fromMillisSinceEpoch(
                                    // TODO: Optimize
                                    SqlParserUtil.parseInteger(s).multiply(new BigDecimal("1000")).longValue()), 0, pos);
                }
                catch (NumberFormatException e) {
                    newNode = visit(SqlParserUtil.parseTimestampLiteral(node.toString(), pos));
                }
            }
            else {
                newNode = new SqlBasicCall(
                        new SqlCastFunction(),
                        (SqlNodeList) visit(SqlNodeList.of(node, new SqlDataTypeSpec(new SqlUserDefinedTypeNameSpec("TIMESTAMP", pos), pos))),
                        pos);
            }
            return newNode;
        }

        private SqlLiteral parseTimeunit(SqlNode node, SqlParserPos pos)
        {
            String timeunit;
            String origIdentifier = ((SqlIntervalQualifier)node).timeUnitRange.toString().toLowerCase();
            if (origIdentifier.equals("yy") || origIdentifier.equals("yyyy")) {
                timeunit = "year";
            }
            else if (origIdentifier.equals("mm") || origIdentifier.equals("m")) {
                timeunit = "month";
            }
            else if (origIdentifier.equals("dd") || origIdentifier.equals("d")) {
                timeunit = "day";
            }
            else if (origIdentifier.equals("hh")) {
                timeunit = "hour";
            }
            else if (origIdentifier.equals("mi") || origIdentifier.equals("n")) {
                timeunit = "minute";
            }
            else if (origIdentifier.equals("ss") || origIdentifier.equals("s")) {
                timeunit = "second";
            }
            else {
                timeunit = origIdentifier;
            }
            return SqlLiteral.createCharString(timeunit, pos);
        }

        @Override
        public @Nullable SqlNode visit(SqlCall call)
        {
            SqlParserPos pos = call.getParserPosition();
            switch (call.getKind()) {
                case CAST: {
                    SqlDataTypeSpec origDstType = call.operand(1);
                    SqlTypeNameSpec dstType;
                    if (origDstType.getTypeNameSpec().getTypeName().getSimple().equalsIgnoreCase("DATETIME")) {
                        dstType = new SqlUserDefinedTypeNameSpec("TIMESTAMP", pos);
                    } else {
                        dstType = origDstType.getTypeNameSpec();
                    }
                    return new SqlBasicCall(
                            new SqlCastFunction(),
                            (SqlNodeList) visit(SqlNodeList.of(call.operand(0), new SqlDataTypeSpec(dstType, pos))),
                            pos);
                }
                case OTHER_FUNCTION: {
                    String funcName = call.getOperator().getName();
                    if (funcName.equalsIgnoreCase("DATEADD")) {
                        SqlLiteral timeunit = parseTimeunit(call.operand(0), pos);
                        SqlNode diff = parseTimeStampValue(call.operand(1), pos);
                        SqlNode target = parseTimeStampValue(call.operand(2), pos);
                        return new SqlBasicCall(
                                new SqlUnresolvedFunction(
                                        new SqlIdentifier("DATE_ADD", pos),
                                        null, null, null, null, SqlFunctionCategory.TIMEDATE
                                ),
                                SqlNodeList.of(timeunit, diff, target), pos);
                    }
                    else if (funcName.equalsIgnoreCase("DATEDIFF")) {
                        SqlLiteral timeunit = parseTimeunit(call.operand(0), pos);
                        SqlNode v1 = parseTimeStampValue(call.operand(1), pos);
                        SqlNode v2 = parseTimeStampValue(call.operand(2), pos);
                        return new SqlBasicCall(
                                new SqlUnresolvedFunction(
                                        new SqlIdentifier("DATE_DIFF", pos),
                                        null, null, null, null, SqlFunctionCategory.TIMEDATE
                                ),
                                SqlNodeList.of(timeunit, v1, v2), pos);
                    }
                    break;
                }
            }
            return super.visit(call);
        }
    }

    public String rewrite(String origSql) throws SqlParseException {
        SchemaPlus schema = Frameworks.createRootSchema(true);
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .parserConfig(
                        SqlParser.Config.DEFAULT.withParserFactory(SqlBabelParserImpl.FACTORY)
                                .withConformance(SqlConformanceEnum.BABEL)
                                .withQuotedCasing(Casing.UNCHANGED)
                                .withUnquotedCasing(Casing.UNCHANGED)
                ).build();
        Planner planner = Frameworks.getPlanner(config);

        if (PATTERN_EXECUTE.matcher(origSql).find()) {
            return origSql;
        }
        // FIXME
        if (!origSql.contains("[") && !origSql.contains("]")) {
            return origSql;
        }

        String escapedPrefixSql = null;
        Matcher matcher = PATTERN_PREPARE.matcher(origSql);
        String sql;
        if (matcher.find()) {
            escapedPrefixSql = matcher.group(1);
            sql = matcher.group(2);
        }
        else {
            sql = origSql;
        }

        SqlNode node = planner.parse(
                // TODO: Fix this naive way
                sql.replace("[", "").replace("]", "")
                        // TODO: Remove this after https://github.com/apache/calcite/pull/2795 is merged
                        // .replace("DAY(", "DAY_OF_YEAR(")

                        // TODO: Fix this naive way
                        .replace("(S,", "(second,")
                        .replace("(s,", "(second,")
        );
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(config.getTypeSystem());
        SqlRewriter rewriter = new SqlRewriter(config.getOperatorTable(),
                new CalciteCatalogReader(
                        CalciteSchema.from(schema),
                        CalciteSchema.from(schema).path(null),
                        typeFactory,
                        null
                ),
                typeFactory,
                config.getSqlValidatorConfig());

        SqlNode rewritten = rewriter.rewrite(node.accept(new SqlVisitor()));
        String rewrittenSqlStr = rewritten.toSqlString(
                (transform) ->
                    transform.withDialect(PrestoSqlDialect.DEFAULT)
                            .withAlwaysUseParentheses(false)
                            .withSubQueryStyle(SqlWriter.SubQueryStyle.HYDE)
                            .withClauseStartsLine(false)
                            .withClauseEndsLine(false)
        ).toString();

        if (escapedPrefixSql == null) {
            return rewrittenSqlStr;
        }
        else {
            return escapedPrefixSql + rewrittenSqlStr;
        }
    }
}
