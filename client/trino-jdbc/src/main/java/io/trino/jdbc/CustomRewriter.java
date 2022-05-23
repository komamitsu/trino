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
import org.apache.calcite.sql.advise.SqlSimpleParser;
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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CustomRewriter
{
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
        private SqlNode cast(SqlNode node, SqlDataTypeSpec type, SqlParserPos pos)
        {
            SqlNode value;
            if (node.getKind() == SqlKind.LITERAL) {
                value = node;
            }
            else {
                value = visit((SqlCall) node);
            }
            return new SqlBasicCall(
                    new SqlCastFunction(),
                    SqlNodeList.of(value, type),
                    pos);
        }

        private SqlNode castToString(SqlNode node, SqlParserPos pos)
        {
            if (node.getKind() == SqlKind.CAST) {
                SqlBasicCall func = (SqlBasicCall) node;
                SqlDataTypeSpec type = func.operand(1);
                if (type.getTypeName().getSimple().equalsIgnoreCase("TIME")) {
                    return new SqlBasicCall(
                            new SqlUnresolvedFunction(
                                    new SqlIdentifier("DATE_FORMAT", pos),
                                    null, null, null, null, SqlFunctionCategory.STRING
                            ),
                            SqlNodeList.of(
                                    castToTimestamp(node, pos),
                                    SqlLiteral.createCharString("%H:%i:%s", pos)), pos);

                }
            }
            return cast(node, new SqlDataTypeSpec(new SqlUserDefinedTypeNameSpec("VARCHAR", pos), pos), pos);
        }

        private SqlNode castToTimestamp(SqlNode node, SqlParserPos pos)
        {
            return cast(node, new SqlDataTypeSpec(new SqlUserDefinedTypeNameSpec("TIMESTAMP", pos), pos), pos);
        }

        private static final BigDecimal BIG_DECIMAL_THOUSAND = new BigDecimal("1000");
        private SqlNode parseTimeStampValue(SqlNode node, SqlParserPos pos)
        {
            switch (node.getKind()) {
                case LITERAL: {
                    SqlNode newNode;
                    String s = node.toString();
                    try {
                        newNode = SqlTimestampLiteral.createTimestamp(
                                TimestampString.fromMillisSinceEpoch(
                                        SqlParserUtil.parseInteger(s).multiply(BIG_DECIMAL_THOUSAND).longValue()), 0, pos);
                    }
                    catch (NumberFormatException e) {
                        newNode = visit(SqlParserUtil.parseTimestampLiteral(node.toString(), pos));
                    }
                    return newNode;
                }
                case IDENTIFIER: {
                    return node;
                }
                default:
                    return castToTimestamp(node, pos);
            }
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
                        SqlNode diff = visit((SqlCall) call.operand(1));
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
                    else if (funcName.equalsIgnoreCase("DATEPART")) {
                        String timeunit = call.operand(0).toString();
                        SqlNode v = parseTimeStampValue(call.operand(1), pos);
                        // TODO?: Current impl relies on pre-conversion in rewrite()
                        // https://trino.io/docs/current/functions/datetime.html#extraction-function
                        String newFuncName = timeunit;
                        return new SqlBasicCall(
                                new SqlUnresolvedFunction(
                                        new SqlIdentifier(newFuncName, pos),
                                        null, null, null, null, SqlFunctionCategory.NUMERIC
                                ),
                                SqlNodeList.of(v), pos);
                    }
                    else if (funcName.equalsIgnoreCase("CONCAT")) {
                        List<SqlNode> args = new ArrayList<>();
                        for (SqlNode arg : call.getOperandList()) {
                            args.add(castToString(arg, pos));
                        }
                        return new SqlBasicCall(
                                new SqlUnresolvedFunction(
                                        new SqlIdentifier(funcName, pos),
                                        null, null, null, null, SqlFunctionCategory.STRING
                                ),
                                SqlNodeList.of(pos, args), pos);
                    }
                    break;
                }
            }
            return super.visit(call);
        }
    }

    private static final Pattern REGEXP_ID = Pattern.compile("ID\\((.*)\\)");
    private String convertBeforeRewrite(String sql)
    {
        SqlSimpleParser.Tokenizer tokenizer = new SqlSimpleParser.Tokenizer(sql, "");
        StringBuilder sb = new StringBuilder();
        SqlSimpleParser.Token lastToken = null;
        while (true) {
            SqlSimpleParser.Token token = tokenizer.nextToken();
            if (token == null) {
                break;
            }
            Matcher matcher = REGEXP_ID.matcher(token.toString());
            sb.append(" ");
            if (matcher.find()) {
                String id = matcher.group(1);
                if (lastToken != null && (
                        lastToken.toString().equalsIgnoreCase("LPAREN")
                                || lastToken.toString().equalsIgnoreCase("COMMA")
                )) {
                    String tmpId = id.replace("\"", "");
                    if (tmpId.equalsIgnoreCase("yy")) {
                        sb.append("year");
                    }
                    else if (tmpId.equalsIgnoreCase("mm")) {
                        sb.append("month");
                    }
                    else if (tmpId.equalsIgnoreCase("q")) {
                        sb.append("quarter");
                    }
                    else if (tmpId.equalsIgnoreCase("dd")) {
                        sb.append("day");
                    }
                    else if (tmpId.equalsIgnoreCase("s")) {
                        sb.append("second");
                    }
                    else {
                        sb.append(id.replace("[", "\"").replace("]", "\""));
                    }
                }
                else {
                    sb.append(id.replace("[", "\"").replace("]", "\""));
                }
            }
            else {
                token.unparse(sb);
            }
            sb.append(" ");

            lastToken = token;
        }
        return sb.toString();
    }

    private final Pattern PATTERN_PREPARE = Pattern.compile("((?:PREPARE|prepare)\\s+\\w+\\s+(?:FROM|from)\\s+)(.*)", Pattern.DOTALL);
    private final Pattern PATTERN_EXECUTE = Pattern.compile("((?:EXECUTE|execute)\\s+)(.*)", Pattern.DOTALL);
    private final Pattern PATTERN_DESCRIBE = Pattern.compile("((?:DESCRIBE|describe)\\s+)(.*)", Pattern.DOTALL);

    public String rewrite(String origSql)
            throws SqlParseException
    {
        if (PATTERN_EXECUTE.matcher(origSql).find()
                || PATTERN_DESCRIBE.matcher(origSql).find()) {
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

        SqlNode node = planner.parse(convertBeforeRewrite(sql));
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
                            .withClauseStartsLine(true)
                            .withClauseEndsLine(true)
        ).toString();

        if (escapedPrefixSql == null) {
            return rewrittenSqlStr;
        }
        else {
            return escapedPrefixSql + rewrittenSqlStr;
        }
    }
}
