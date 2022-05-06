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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperatorTable;
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
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.checkerframework.checker.nullness.qual.Nullable;

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
                    if (call.getOperator().getName().equalsIgnoreCase("DATEADD")) {
                        String timeunit;
                        String origIdentifier = ((SqlIdentifier)call.getOperandList().get(0)).getSimple().toLowerCase();
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

                        SqlNode origDiff = call.operand(1);
                        SqlNode diff;
                        if (origDiff.getKind() == SqlKind.LITERAL) {
                            diff = visit((SqlNumericLiteral)origDiff);
                        }
                        else {
                            diff = visit((SqlBasicCall)origDiff);
                        }

                        SqlNode origTarget = call.operand(2);
                        SqlNode target;
                        if (origTarget.getKind() == SqlKind.LITERAL) {
                            target = visit(SqlParserUtil.parseTimestampLiteral(origTarget.toString(), pos));
                        }
                        else {
                            target = visit((SqlBasicCall)origTarget);
                        }
                        return new SqlBasicCall(
                                new SqlUnresolvedFunction(
                                        new SqlIdentifier("DATE_ADD", pos),
                                        null, null, null, null, SqlFunctionCategory.TIMEDATE
                                ),
                                SqlNodeList.of(
                                        SqlLiteral.createCharString(timeunit, pos),
                                        diff,
                                        target
                                ),
                                pos
                        );
                    }
                    break;
                }
            }
            return super.visit(call);
        }
    }

    private final Pattern PATTERN_PREPARE = Pattern.compile("((?:PREPARE|prepare)\\s+\\w+\\s+(?:FROM|from)\\s+)(.*)", Pattern.DOTALL);
    private final Pattern PATTERN_EXECUTE = Pattern.compile("((?:EXECUTE|execute)\\s+)(.*)", Pattern.DOTALL);
    public String rewrite(String origSql) throws SqlParseException {
        SchemaPlus schema = Frameworks.createRootSchema(true);
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .sqlValidatorConfig(SqlValidator.Config.DEFAULT)
                .parserConfig(
                        SqlParser.config()
                                //   .withQuoting(Quoting.BRACKET)
                                .withCaseSensitive(true)
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

        // TODO: Fix this naive way
        SqlNode node = planner.parse(sql.replace("[", "").replace("]", ""));
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
