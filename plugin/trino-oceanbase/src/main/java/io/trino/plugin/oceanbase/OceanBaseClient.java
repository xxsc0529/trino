/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.oceanbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.BooleanWriteFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.StandardColumnMappings;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementCovariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementCovarianceSamp;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.stream.Collectors.joining;

public class OceanBaseClient
        extends BaseJdbcClient
{
    private static final int TYPE_BINARY_FLOAT = 100;
    private static final int TYPE_BINARY_DOUBLE = 101;

    private static final int ZERO_PRECISION_TIME_COLUMN_SIZE = 10;
    private static final int ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE = 19;
    private static final int MYSQL_MODE_MAX_TIMESTAMP_PRECISION = 6;
    private static final int ORACLE_MODE_MAX_TIMESTAMP_PRECISION = 9;

    private static final int BYTES_PER_CHAR = 4;
    private static final int MYSQL_MODE_CHAR_MAX_LENGTH = 256;
    private static final int ORACLE_MODE_CHAR_MAX_BYTES = 2000;
    private static final int ORACLE_MODE_CHAR_MAX_LENGTH = ORACLE_MODE_CHAR_MAX_BYTES / BYTES_PER_CHAR;
    private static final int VARCHAR2_MAX_BYTES = 32767;
    private static final int VARCHAR2_MAX_LENGTH = VARCHAR2_MAX_BYTES / BYTES_PER_CHAR;
    private static final int TINYTEXT_MAX_BYTES = 255;
    private static final int TEXT_MAX_BYTES = 65535;
    private static final int MEDIUMTEXT_MAX_BYTES = 16777215;

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");
    private static final DateTimeFormatter TIMESTAMP_SECONDS_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP_NANO_OPTIONAL_FORMATTER = new DateTimeFormatterBuilder().appendPattern("uuuu-MM-dd HH:mm:ss").optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd().toFormatter();

    private static final Set<String> INTERNAL_DATABASES = ImmutableSet.<String>builder().add("information_schema").add("mysql").add("oceanbase").build();

    private static final Set<String> INTERNAL_SCHEMAS = ImmutableSet.<String>builder().add("SYS").add("LBACSYS").add("ORAAUDITOR").build();

    private final OceanBaseCompatibleMode compatibleMode;
    private final Type jsonType;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    @Inject
    public OceanBaseClient(BaseJdbcConfig config, OceanBaseConfig obConfig, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, TypeManager typeManager, IdentifierMapping identifierMapping, RemoteQueryModifier queryModifier)
    {
        super(obConfig.getCompatibleMode().isMySQLMode() ? "`" : "\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);

        this.compatibleMode = obConfig.getCompatibleMode();

        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder().addStandardRules(this::quoted).withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "real", "double")).map("$equal(left: numeric_type, right: numeric_type)").to("left = right").map("$not_equal(left: numeric_type, right: numeric_type)").to("left <> right").map("$less_than(left: numeric_type, right: numeric_type)").to("left < right").map("$less_than_or_equal(left: numeric_type, right: numeric_type)").to("left <= right").map("$greater_than(left: numeric_type, right: numeric_type)").to("left > right").map("$greater_than_or_equal(left: numeric_type, right: numeric_type)").to("left >= right").build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(connectorExpressionRewriter, ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder().add(new ImplementCountAll(bigintTypeHandle)).add(new ImplementCount(bigintTypeHandle)).add(new ImplementCountDistinct(bigintTypeHandle, true)).add(new ImplementMinMax(true)).add(new ImplementSum(this::toTypeHandle)).add(new ImplementAvgFloatingPoint()).add(new ImplementAvgDecimal()).add(new ImplementStddevSamp()).add(new ImplementStddevPop()).add(new ImplementVarianceSamp()).add(new ImplementVariancePop()).add(new ImplementCovarianceSamp()).add(new ImplementCovariancePop()).build());
    }

    private Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        else if (!isNullOrEmpty(catalog)) {
            sb.append(quoted(catalog)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (columnCount.isPresent()) {
            statement.setFetchSize(max(100_000 / columnCount.get(), 1_000));
        }
        return statement;
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return compatibleMode.isMySQLMode() ? resultSet.getString("TABLE_CAT") : resultSet.getString("TABLE_SCHEM");
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            try (ResultSet resultSet = compatibleMode.isMySQLMode() ? databaseMetaData.getCatalogs() : databaseMetaData.getSchemas()) {
                ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
                while (resultSet.next()) {
                    String schemaName = getTableSchemaName(resultSet);
                    // skip internal schemas
                    if (filterSchema(schemaName)) {
                        schemaNames.add(schemaName);
                    }
                }
                return schemaNames.build();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected boolean filterSchema(String schemaName)
    {
        return compatibleMode.isMySQLMode() ? !INTERNAL_DATABASES.contains(schemaName.toLowerCase(Locale.ENGLISH)) : !INTERNAL_SCHEMAS.contains(schemaName.toUpperCase(Locale.ENGLISH));
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        if (!compatibleMode.isMySQLMode()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas on Oracle mode");
        }
        super.createSchema(session, schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (!compatibleMode.isMySQLMode()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas on Oracle mode");
        }
        super.dropSchema(session, schemaName, cascade);
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        if (!cascade) {
            try (ResultSet tables = getTables(connection, Optional.of(remoteSchemaName), Optional.empty())) {
                if (tables.next()) {
                    throw new TrinoException(SCHEMA_NOT_EMPTY, "Cannot drop non-empty schema '%s'".formatted(remoteSchemaName));
                }
            }
        }
        execute(session, connection, "DROP SCHEMA " + quoted(remoteSchemaName));
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String schemaName = escapeObjectNameForMetadataQuery(remoteSchemaName, metadata.getSearchStringEscape()).orElse(null);
        return metadata.getTables(compatibleMode.isMySQLMode() ? schemaName : null, compatibleMode.isMySQLMode() ? null : schemaName, escapeObjectNameForMetadataQuery(remoteTableName, metadata.getSearchStringEscape()).orElse(null), getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        if (column.getComment() != null) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
        }

        return "%s %s %s".formatted(quoted(columnName), toWriteMapping(session, column.getType()).getDataType(), column.isNullable() ? compatibleMode.isMySQLMode() ? "NULL" : "" : "NOT NULL");
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        ImmutableList.Builder<String> createTableSqlsBuilder = ImmutableList.builder();
        createTableSqlsBuilder.add(format("CREATE TABLE %s (%s)", quoted(remoteTableName), join(", ", columns)));
        Optional<String> tableComment = tableMetadata.getComment();
        if (tableComment.isPresent()) {
            createTableSqlsBuilder.add(buildTableCommentSql(remoteTableName, tableComment));
        }
        return createTableSqlsBuilder.build();
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        execute(session, buildTableCommentSql(handle.asPlainTable().getRemoteTableName(), comment));
    }

    private String buildTableCommentSql(RemoteTableName remoteTableName, Optional<String> comment)
    {
        return compatibleMode.isMySQLMode() ? format("COMMENT %s", varcharLiteral(comment.orElse(""))) : format("COMMENT ON TABLE %s IS %s", quoted(remoteTableName), varcharLiteral(comment.orElse("")));
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        String catalogName = remoteTableName.getCatalogName().orElse(null);
        String schemaName = remoteTableName.getSchemaName().orElse(null);
        if (compatibleMode.isMySQLMode()) {
            verify(schemaName == null);
            renameTable(session, null, catalogName, remoteTableName.getTableName(), newTableName);
        }
        else {
            renameTable(session, catalogName, schemaName, remoteTableName.getTableName(), newTableName);
        }
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        if (!remoteSchemaName.equals(newRemoteSchemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }

        execute(session, connection, format("ALTER TABLE %s RENAME TO %s", quoted(catalogName, remoteSchemaName, remoteTableName), quoted(compatibleMode.isMySQLMode() ? newRemoteTableName : newRemoteTableName.toUpperCase(ENGLISH))));
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        execute(session, connection, format("ALTER TABLE %s RENAME COLUMN %s TO %s", quoted(remoteTableName.getCatalogName().orElse(null), remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()), quoted(remoteColumnName), quoted(newRemoteColumnName)));
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        if (!compatibleMode.isMySQLMode()) {
            return false;
        }
        for (JdbcSortItem sortItem : sortOrder) {
            Type sortItemType = sortItem.column().getColumnType();
            if (sortItemType instanceof CharType || sortItemType instanceof VarcharType) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return compatibleMode.isMySQLMode() ? Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream().flatMap(sortItem -> {
                String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                String columnSorting = format("%s %s", quoted(sortItem.column().getColumnName()), ordering);
                return switch (sortItem.sortOrder()) {
                    case ASC_NULLS_FIRST, DESC_NULLS_LAST -> Stream.of(columnSorting);
                    case ASC_NULLS_LAST -> Stream.of(format("ISNULL(%s) ASC", quoted(sortItem.column().getColumnName())), columnSorting);
                    case DESC_NULLS_FIRST -> Stream.of(format("ISNULL(%s) DESC", quoted(sortItem.column().getColumnName())), columnSorting);
                };
            }).collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %s", query, orderBy, limit);
        }) : Optional.empty();
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        if (compatibleMode.isMySQLMode()) {
            return true;
        }
        throw new UnsupportedOperationException("isTopNGuaranteed is not implemented on Oracle mode");
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> compatibleMode.isMySQLMode() ? sql + " LIMIT " + limit : format("SELECT * FROM (%s) WHERE ROWNUM <= %s", sql, limit));
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return !compatibleMode.isMySQLMode() || Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn()).map(JdbcColumnHandle::getColumnType).noneMatch(type -> type instanceof CharType || type instanceof VarcharType);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.jdbcTypeName().orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        return switch (jdbcTypeName.toLowerCase(ENGLISH)) {
            case "tinyint unsigned" -> Optional.of(smallintColumnMapping());
            case "smallint unsigned", "year" -> Optional.of(integerColumnMapping());
            case "int unsigned" -> Optional.of(bigintColumnMapping());
            case "bigint unsigned" -> Optional.of(StandardColumnMappings.decimalColumnMapping(createDecimalType(20)));
            case "date" -> Optional.of(dateColumnMapping());
            case "json" -> Optional.of(jsonColumnMapping());
            case "enum", "set" -> Optional.of(defaultVarcharColumnMapping(typeHandle.columnSize().orElse(MYSQL_MODE_CHAR_MAX_LENGTH), false));
            case "datetime" -> Optional.of(timestampColumnMapping(typeHandle.requiredColumnSize(), typeHandle.decimalDigits().orElse(0)));
            default -> switch (typeHandle.jdbcType()) {
                case Types.BIT -> Optional.of(booleanColumnMapping());
                case Types.TINYINT -> Optional.of(tinyintColumnMapping());
                case Types.SMALLINT -> Optional.of(smallintColumnMapping());
                case Types.INTEGER -> Optional.of(integerColumnMapping());
                case Types.BIGINT -> Optional.of(bigintColumnMapping());
                case Types.REAL, TYPE_BINARY_FLOAT -> Optional.of(floatColumnMapping());
                case Types.DOUBLE, TYPE_BINARY_DOUBLE -> Optional.of(doubleColumnMapping());
                case Types.NUMERIC, Types.DECIMAL -> Optional.ofNullable(numericColumnMapping(session, typeHandle));
                case Types.CHAR, Types.NCHAR -> Optional.of(defaultCharColumnMapping(typeHandle.requiredColumnSize(), false));
                case Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR -> Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), false));
                case Types.CLOB -> Optional.of(clobColumnMapping());
                case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> Optional.of(varbinaryColumnMapping());
                case Types.TIME -> Optional.of(timeColumnMapping(typeHandle.requiredColumnSize()));
                case Types.TIMESTAMP ->
                        compatibleMode.isMySQLMode() ? Optional.of(timestampWithTimeZoneColumnMapping(typeHandle.requiredColumnSize(), typeHandle.decimalDigits().orElse(0))) : Optional.of(timestampColumnMapping(typeHandle.requiredColumnSize(), typeHandle.decimalDigits().orElse(0)));
                default -> Optional.empty();
            };
        };
    }

    private ColumnMapping dateColumnMapping()
    {
        return compatibleMode.isMySQLMode() ? ColumnMapping.longMapping(DATE, dateReadFunctionUsingLocalDate(), dateWriteFunction(), FULL_PUSHDOWN) : ColumnMapping.longMapping(TIMESTAMP_SECONDS, dateToTimestampReadFunction(), timestampToDateWriteFunction(), FULL_PUSHDOWN);
    }

    private LongReadFunction dateToTimestampReadFunction()
    {
        return (resultSet, columnIndex) -> {
            LocalDateTime timestamp = resultSet.getObject(columnIndex, LocalDateTime.class);
            // Adjust years when the value is B.C. dates because Oracle returns +1 year unless converting to string in their server side
            if (timestamp.getYear() <= 0) {
                timestamp = timestamp.minusYears(1);
            }
            return toTrinoTimestamp(TIMESTAMP_SECONDS, timestamp);
        };
    }

    private String getToDateExpression()
    {
        return compatibleMode.isMySQLMode() ? "CAST(? AS DATE)" : "TO_DATE(?, 'SYYYY-MM-DD HH24:MI:SS')";
    }

    private LongWriteFunction dateWriteFunction()
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return getToDateExpression();
            }

            @Override
            public void set(PreparedStatement statement, int index, long value)
                    throws SQLException
            {
                if (compatibleMode.isMySQLMode()) {
                    statement.setString(index, LocalDate.ofEpochDay(value).format(ISO_DATE));
                }
                else {
                    long utcMillis = DAYS.toMillis(value);
                    LocalDateTime date = LocalDateTime.from(Instant.ofEpochMilli(utcMillis).atZone(ZoneOffset.UTC));
                    statement.setString(index, DATE_FORMATTER.format(date));
                }
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                if (compatibleMode.isMySQLMode()) {
                    statement.setObject(index, null);
                }
                else {
                    statement.setNull(index, Types.VARCHAR);
                }
            }
        };
    }

    private LongWriteFunction timestampToDateWriteFunction()
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return getToDateExpression();
            }

            @Override
            public void set(PreparedStatement statement, int index, long value)
                    throws SQLException
            {
                long epochSecond = floorDiv(value, MICROSECONDS_PER_SECOND);
                int microsOfSecond = floorMod(value, MICROSECONDS_PER_SECOND);
                verify(microsOfSecond == 0, "Micros of second must be zero: '%s'", value);
                LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC);
                statement.setString(index, TIMESTAMP_SECONDS_FORMATTER.format(localDateTime));
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(jsonType, (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))), varcharWriteFunction(), DISABLE_PUSHDOWN);
    }

    private ColumnMapping floatColumnMapping()
    {
        return ColumnMapping.longMapping(REAL, (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)), realWriteFunction(), DISABLE_PUSHDOWN);
    }

    private ColumnMapping numericColumnMapping(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        int precision = typeHandle.requiredColumnSize();
        if (precision == 0) {
            if (getDecimalRounding(session) == ALLOW_OVERFLOW) {
                DecimalType decimalType = createDecimalType(Decimals.MAX_PRECISION, getDecimalDefaultScale(session));
                return ColumnMapping.objectMapping(decimalType, longDecimalReadFunction(decimalType, getDecimalRoundingMode(session)), longDecimalWriteFunction(decimalType), FULL_PUSHDOWN);
            }
            return null;
        }

        int decimalDigits = typeHandle.requiredDecimalDigits();
        int width = precision + max(-decimalDigits, 0);
        int scale = max(decimalDigits, 0);

        if (width <= Decimals.MAX_PRECISION) {
            return StandardColumnMappings.decimalColumnMapping(createDecimalType(width, scale));
        }
        return getDecimalRounding(session) == ALLOW_OVERFLOW ? StandardColumnMappings.decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)) : null;
    }

    private ColumnMapping clobColumnMapping()
    {
        return ColumnMapping.sliceMapping(createUnboundedVarcharType(), (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)), varcharWriteFunction(), DISABLE_PUSHDOWN);
    }

    private int getMaxTimestampPrecision()
    {
        return compatibleMode.isMySQLMode() ? MYSQL_MODE_MAX_TIMESTAMP_PRECISION : ORACLE_MODE_MAX_TIMESTAMP_PRECISION;
    }

    private void verifyTimestampPrecision(int precision)
    {
        verify(1 <= precision && precision <= getMaxTimestampPrecision(), "Unexpected timestamp precision %s", precision);
    }

    private int getTimePrecision(int timeColumnSize)
    {
        if (timeColumnSize == ZERO_PRECISION_TIME_COLUMN_SIZE) {
            return 0;
        }
        int timePrecision = timeColumnSize - ZERO_PRECISION_TIME_COLUMN_SIZE - 1;
        verifyTimestampPrecision(timePrecision);
        return timePrecision;
    }

    private int getTimestampPrecision(int timestampColumnSize, int scale)
    {
        if (timestampColumnSize < ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE) {
            verifyTimestampPrecision(scale);
            return scale;
        }
        else if (timestampColumnSize == ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE) {
            return 0;
        }
        else {
            int timestampPrecision = timestampColumnSize - ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE - 1;
            verifyTimestampPrecision(timestampPrecision);
            return timestampPrecision;
        }
    }

    private ColumnMapping timeColumnMapping(int timeColumnSize)
    {
        TimeType timeType = createTimeType(getTimePrecision(timeColumnSize));
        return StandardColumnMappings.timeColumnMapping(timeType);
    }

    private String getToTimestampExpression(int precision)
    {
        if (precision == 0) {
            return "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS')";
        }
        if (precision <= 2) {
            return "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF')";
        }
        return format("TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF%d')", precision);
    }

    private ColumnMapping timestampColumnMapping(int timestampColumnSize, int scale)
    {
        TimestampType timestampType = createTimestampType(getTimestampPrecision(timestampColumnSize, scale));
        if (timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(timestampType, timestampReadFunction(timestampType), timestampWriteFunction(timestampType));
        }
        return ColumnMapping.objectMapping(timestampType, longTimestampReadFunction(timestampType), longTimestampWriteFunction(timestampType));
    }

    private LongWriteFunction timestampWriteFunction(TimestampType timestampType)
    {
        return compatibleMode.isMySQLMode() ? StandardColumnMappings.timestampWriteFunction(timestampType) : new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return getToTimestampExpression(timestampType.getPrecision());
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochMicros)
                    throws SQLException
            {
                LocalDateTime timestamp = fromTrinoTimestamp(epochMicros);
                statement.setString(index, TIMESTAMP_NANO_OPTIONAL_FORMATTER.format(timestamp));
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private ObjectWriteFunction longTimestampWriteFunction(TimestampType timestampType)
    {
        return compatibleMode.isMySQLMode() ? StandardColumnMappings.longTimestampWriteFunction(timestampType, MYSQL_MODE_MAX_TIMESTAMP_PRECISION) : new ObjectWriteFunction()
        {
            @Override
            public Class<?> getJavaType()
            {
                return LongTimestamp.class;
            }

            @Override
            public String getBindExpression()
            {
                return getToTimestampExpression(timestampType.getPrecision());
            }

            @Override
            public void set(PreparedStatement statement, int index, Object value)
                    throws SQLException
            {
                LocalDateTime timestamp = fromLongTrinoTimestamp((LongTimestamp) value, timestampType.getPrecision());
                statement.setString(index, TIMESTAMP_NANO_OPTIONAL_FORMATTER.format(timestamp));
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private ColumnMapping timestampWithTimeZoneColumnMapping(int timestampColumnSize, int scale)
    {
        TimestampWithTimeZoneType trinoType = createTimestampWithTimeZoneType(getTimestampPrecision(timestampColumnSize, scale));
        if (trinoType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(trinoType, shortTimestampWithTimeZoneReadFunction(), shortTimestampWithTimeZoneWriteFunction());
        }
        return ColumnMapping.objectMapping(trinoType, longTimestampWithTimeZoneReadFunction(), longTimestampWithTimeZoneWriteFunction());
    }

    private LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            Timestamp timestamp = resultSet.getTimestamp(columnIndex);
            long millisUtc = timestamp.getTime();
            return packDateTimeWithZone(millisUtc, UTC_KEY);
        };
    }

    private LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            Instant instantValue = Instant.ofEpochMilli(unpackMillisUtc(value));
            statement.setObject(index, instantValue);
        };
    }

    private ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(LongTimestampWithTimeZone.class, (resultSet, columnIndex) -> {
            OffsetDateTime offsetDateTime = resultSet.getObject(columnIndex, OffsetDateTime.class);
            return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(offsetDateTime.toEpochSecond(), (long) offsetDateTime.getNano() * PICOSECONDS_PER_NANOSECOND, UTC_KEY);
        });
    }

    private ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            long epochSeconds = floorDiv(value.getEpochMillis(), MILLISECONDS_PER_SECOND);
            long nanosOfSecond = (long) floorMod(value.getEpochMillis(), MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
            Instant instantValue = Instant.ofEpochSecond(epochSeconds, nanosOfSecond);
            statement.setObject(index, instantValue);
        });
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return compatibleMode.isMySQLMode() ? WriteMapping.booleanMapping("boolean", booleanWriteFunction()) : WriteMapping.booleanMapping("number(1)", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return compatibleMode.isMySQLMode() ? WriteMapping.longMapping("tinyint", tinyintWriteFunction()) : WriteMapping.longMapping("number(3)", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return compatibleMode.isMySQLMode() ? WriteMapping.longMapping("smallint", smallintWriteFunction()) : WriteMapping.longMapping("number(5)", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return compatibleMode.isMySQLMode() ? WriteMapping.longMapping("integer", integerWriteFunction()) : WriteMapping.longMapping("number(10)", integerWriteFunction());
        }
        if (type == BIGINT) {
            return compatibleMode.isMySQLMode() ? WriteMapping.longMapping("bigint", bigintWriteFunction()) : WriteMapping.longMapping("number(19)", bigintWriteFunction());
        }
        if (type == REAL) {
            return compatibleMode.isMySQLMode() ? WriteMapping.longMapping("float", realWriteFunction()) : WriteMapping.longMapping("binary_float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return compatibleMode.isMySQLMode() ? WriteMapping.doubleMapping("double precision", doubleWriteFunction()) : WriteMapping.doubleMapping("binary_double", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format(compatibleMode.isMySQLMode() ? "decimal(%s, %s)" : "number(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof CharType charType) {
            String dataType;
            if (compatibleMode.isMySQLMode()) {
                dataType = charType.getLength() < MYSQL_MODE_CHAR_MAX_LENGTH ? "char(" + charType.getLength() + ")" : "clob";
            }
            else {
                dataType = charType.getLength() < ORACLE_MODE_CHAR_MAX_LENGTH ? "char(" + charType.getLength() + " CHAR)" : "clob";
            }
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            return WriteMapping.sliceMapping(getVarcharDataType(varcharType), varcharWriteFunction());
        }

        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("json", varcharWriteFunction());
        }

        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("blob", varbinaryWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunction());
        }

        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= MYSQL_MODE_MAX_TIMESTAMP_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", MYSQL_MODE_MAX_TIMESTAMP_PRECISION), timeWriteFunction(MYSQL_MODE_MAX_TIMESTAMP_PRECISION));
        }

        if (type instanceof TimestampType timestampType) {
            if (compatibleMode.isMySQLMode()) {
                if (timestampType.getPrecision() <= MYSQL_MODE_MAX_TIMESTAMP_PRECISION) {
                    return WriteMapping.longMapping(format("datetime(%s)", timestampType.getPrecision()), timestampWriteFunction(timestampType));
                }
                return WriteMapping.objectMapping(format("datetime(%s)", MYSQL_MODE_MAX_TIMESTAMP_PRECISION), longTimestampWriteFunction(timestampType));
            }
            else {
                if (type.equals(TIMESTAMP_SECONDS)) {
                    return WriteMapping.longMapping("date", timestampToDateWriteFunction());
                }
                int precision = min(timestampType.getPrecision(), ORACLE_MODE_MAX_TIMESTAMP_PRECISION);
                String dataType = format("timestamp(%d)", precision);
                if (timestampType.isShort()) {
                    return WriteMapping.longMapping(dataType, timestampWriteFunction(timestampType));
                }
                return WriteMapping.objectMapping(dataType, longTimestampWriteFunction(createTimestampType(precision)));
            }
        }

        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.getPrecision() <= MYSQL_MODE_MAX_TIMESTAMP_PRECISION) {
                String dataType = format("timestamp(%d)", timestampWithTimeZoneType.getPrecision());
                if (timestampWithTimeZoneType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                    return WriteMapping.longMapping(dataType, shortTimestampWithTimeZoneWriteFunction());
                }
                return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction());
            }
            return WriteMapping.objectMapping(format("timestamp(%d)", MYSQL_MODE_MAX_TIMESTAMP_PRECISION), longTimestampWithTimeZoneWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private String getVarcharDataType(VarcharType varcharType)
    {
        if (compatibleMode.isMySQLMode()) {
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > MEDIUMTEXT_MAX_BYTES) {
                return "longtext";
            }
            else if (varcharType.getBoundedLength() <= TINYTEXT_MAX_BYTES) {
                return "tinytext";
            }
            else if (varcharType.getBoundedLength() <= TEXT_MAX_BYTES) {
                return "text";
            }
            else {
                return "mediumtext";
            }
        }
        else {
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > VARCHAR2_MAX_LENGTH) {
                return "clob";
            }
            else {
                return "varchar2(" + varcharType.getBoundedLength() + " CHAR)";
            }
        }
    }

    private BooleanWriteFunction booleanWriteFunction()
    {
        return BooleanWriteFunction.of(Types.TINYINT, (statement, index, value) -> statement.setObject(index, value ? 1 : 0));
    }
}
