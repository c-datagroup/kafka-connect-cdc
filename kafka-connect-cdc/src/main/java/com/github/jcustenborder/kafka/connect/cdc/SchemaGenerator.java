/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import freemarker.cache.StringTemplateLoader;
import freemarker.core.InvalidReferenceException;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class SchemaGenerator {
  private static final Logger log = LoggerFactory.getLogger(SchemaGenerator.class);
  final CDCSourceConnectorConfig config;
  final Cache<ChangeKey, SchemaPair> schemaPairCache;
  final Cache<ChangeKey, String> topicNameCache;
  final Configuration configuration;
  final StringTemplateLoader loader;

  final Template namespaceTemplate;
  final Template keyTemplate;
  final Template valueTemplate;
  final Template topicNameTemplate;


  public SchemaGenerator(CDCSourceConnectorConfig config) {
    this.config = config;
    this.schemaPairCache = CacheBuilder.newBuilder()
        .expireAfterWrite(this.config.schemaCacheMs, TimeUnit.MILLISECONDS)
        .build();
    this.topicNameCache = CacheBuilder.newBuilder()
        .expireAfterWrite(this.config.schemaCacheMs, TimeUnit.MILLISECONDS)
        .build();

    this.configuration = new Configuration(Configuration.getVersion());
    this.loader = new StringTemplateLoader();
    this.configuration.setTemplateLoader(this.loader);
    this.configuration.setDefaultEncoding("UTF-8");
    this.configuration.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    this.configuration.setLogTemplateExceptions(false);

    this.namespaceTemplate = loadTemplate(CDCSourceConnectorConfig.NAMESPACE_CONFIG, this.config.namespace);
    this.keyTemplate = loadTemplate(CDCSourceConnectorConfig.KEY_NAME_FORMAT_CONFIG, this.config.keyNameFormat);
    this.valueTemplate = loadTemplate(CDCSourceConnectorConfig.VALUE_NAME_FORMAT_CONFIG, this.config.valueNameFormat);
    this.topicNameTemplate = loadTemplate(CDCSourceConnectorConfig.TOPIC_FORMAT_CONFIG, this.config.topicFormat);
  }

  static String convertCase(String text, CDCSourceConnectorConfig.CaseFormat inputCaseFormat, CDCSourceConnectorConfig.CaseFormat outputCaseFormat) {
    if (Strings.isNullOrEmpty(text)) {
      return "";
    }
    if (CDCSourceConnectorConfig.CaseFormat.LOWER == outputCaseFormat) {
      return text.toLowerCase();
    } else if (CDCSourceConnectorConfig.CaseFormat.UPPER == outputCaseFormat) {
      return text.toUpperCase();
    } else if (CDCSourceConnectorConfig.CaseFormat.NONE == outputCaseFormat) {
      return text;
    }

    CaseFormat inputFormat = caseFormat(inputCaseFormat);
    CaseFormat outputFormat = caseFormat(outputCaseFormat);

    return inputFormat.to(outputFormat, text);
  }

  private static CaseFormat caseFormat(CDCSourceConnectorConfig.CaseFormat inputCaseFormat) {
    CaseFormat inputFormat;
    switch (inputCaseFormat) {
      case LOWER_CAMEL:
        inputFormat = CaseFormat.LOWER_CAMEL;
        break;
      case LOWER_HYPHEN:
        inputFormat = CaseFormat.LOWER_HYPHEN;
        break;
      case LOWER_UNDERSCORE:
        inputFormat = CaseFormat.LOWER_UNDERSCORE;
        break;
      case UPPER_CAMEL:
        inputFormat = CaseFormat.UPPER_CAMEL;
        break;
      case UPPER_UNDERSCORE:
        inputFormat = CaseFormat.UPPER_UNDERSCORE;
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("'%s' is not a supported case format.", inputCaseFormat)
        );
    }
    return inputFormat;
  }

  final Template loadTemplate(String templateName, String template) {
    log.trace("loadTemplate() - Adding templateName '{}' template '{}'", templateName, template);
    this.loader.putTemplate(templateName, template);
    try {
      log.info("Loading template '{}'", templateName);
      return this.configuration.getTemplate(templateName);
    } catch (IOException ex) {
      throw new DataException(
          String.format("Exception thrown while loading template '%s'", templateName),
          ex
      );
    }
  }

  Map<String, String> values(Change change, String namespace) {
    Map<String, String> values = new HashMap<>(Strings.isNullOrEmpty(namespace) ? 3 : 4);

    values.put(
        Constants.NAMESPACE_VARIABLE,
        convertCase(namespace, CDCSourceConnectorConfig.CaseFormat.NONE, CDCSourceConnectorConfig.CaseFormat.NONE)
    );
    values.put(
        Constants.DATABASE_NAME_VARIABLE,
        convertCase(change.databaseName(), this.config.schemaInputFormat, this.config.schemaDatabaseNameFormat)
    );
    values.put(
        Constants.SCHEMA_NAME_VARIABLE,
        convertCase(change.schemaName(), this.config.schemaInputFormat, this.config.schemaSchemaNameFormat)
    );
    values.put(
        Constants.TABLE_NAME_VARIABLE,
        convertCase(change.tableName(), this.config.schemaInputFormat, this.config.schemaTableNameFormat)
    );
    return values;
  }

  String renderTemplate(Change change, Template template, String namespace) {
    Map<String, String> value = values(change, namespace);

    try (StringWriter writer = new StringWriter()) {
      template.process(value, writer);
      return writer.toString();
    } catch (IOException e) {
      throw new DataException("Exception while processing template", e);
    } catch (InvalidReferenceException e) {
      throw new DataException(
          String.format(
              "Exception thrown while processing template. Offending expression '%s'",
              e.getBlamedExpressionString()
          ),
          e);
    } catch (TemplateException e) {
      throw new DataException("Exception while processing template", e);
    }
  }

  String keySchemaName(Change change) {
    String namespace = namespace(change);
    return renderTemplate(change, this.keyTemplate, namespace);
  }

  String valueSchemaName(Change change) {
    String namespace = namespace(change);
    return renderTemplate(change, this.valueTemplate, namespace);
  }

  String namespace(Change change) {
    return renderTemplate(change, this.namespaceTemplate, null);
  }

  String fieldName(Change.ColumnValue columnValue) {
    String fieldName = convertCase(columnValue.columnName(), this.config.schemaInputFormat, this.config.schemaColumnNameFormat);
    return fieldName;
  }

  void addFields(List<Change.ColumnValue> columnValues, List<String> fieldNames, SchemaBuilder builder) {
    for (Change.ColumnValue columnValue : columnValues) {
      Preconditions.checkNotNull(columnValue.schema(), "schema() for %s cannot be null", columnValue.columnName());
      Preconditions.checkNotNull(columnValue.schema().parameters(), "schema().parameters() for %s cannot be null", columnValue.columnName());

      Preconditions.checkState(
          columnValue.schema().parameters().containsKey(Change.ColumnValue.COLUMN_NAME),
          "The schema.parameters() for field(%s) does not contain a value for %s.",
          columnValue.columnName(),
          Change.ColumnValue.COLUMN_NAME
      );
      String fieldName = fieldName(columnValue);
      fieldNames.add(fieldName);
      builder.field(fieldName, columnValue.schema());
    }
  }

  Schema generateValueSchema(Change change, List<String> schemaFields) {
    SchemaBuilder builder = SchemaBuilder.struct();
    String schemaName = valueSchemaName(change);
    builder.name(schemaName);
    addFields(change.valueColumns(), schemaFields, builder);
    builder.field(Constants.METADATA_FIELD, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA));
    builder.parameters(
        ImmutableMap.of(
            Change.DATABASE_NAME, change.databaseName(),
            Change.SCHEMA_NAME, change.schemaName(),
            Change.TABLE_NAME, change.tableName()
        )
    );
    return builder.build();
  }

  Schema generateKeySchema(Change change, List<String> schemaFields) {
    SchemaBuilder builder = SchemaBuilder.struct();
    String schemaName = keySchemaName(change);
    builder.name(schemaName);
    addFields(change.keyColumns(), schemaFields, builder);
    builder.parameters(
        ImmutableMap.of(
            Change.DATABASE_NAME, change.databaseName(),
            Change.SCHEMA_NAME, change.schemaName(),
            Change.TABLE_NAME, change.tableName()
        )
    );


    return builder.build();
  }


  SchemaPair generateSchemas(Change change) {

    log.debug("generateSchemas with change: " + change);
    List<String> keySchemaFields = new ArrayList<>();
    Schema keySchema = generateKeySchema(change, keySchemaFields);
    List<String> valueSchemaFields = new ArrayList<>();
    Schema valueSchema = generateValueSchema(change, valueSchemaFields);
    return new SchemaPair(
        new SchemaAndFields(keySchema, keySchemaFields),
        new SchemaAndFields(valueSchema, valueSchemaFields)
    );
  }

  String generateTopicName(Change change) {
    return renderTemplate(change, this.topicNameTemplate, null);
  }

  public SchemaPair schemas(final Change change) {
    Preconditions.checkNotNull(change, "change cannot be null.");
    Preconditions.checkNotNull(change.databaseName(), "change.databaseName() cannot be null.");
    Preconditions.checkNotNull(change.schemaName(), "change.schemaName() cannot be null.");
    Preconditions.checkNotNull(change.tableName(), "change.tableName() cannot be null.");
    Preconditions.checkNotNull(change.metadata(), "change.metadata() cannot be null.");

    ChangeKey changeKey = new ChangeKey(change);
    try {
      return this.schemaPairCache.get(changeKey, () -> generateSchemas(change));
    } catch (ExecutionException e) {
      throw new DataException("Exception thrown while building schemas.", e);
    }
  }

  public String topic(final Change change) {
    Preconditions.checkNotNull(change, "change cannot be null.");
    Preconditions.checkNotNull(change.databaseName(), "change.databaseName() cannot be null.");
    Preconditions.checkNotNull(change.schemaName(), "change.schemaName() cannot be null.");
    Preconditions.checkNotNull(change.tableName(), "change.tableName() cannot be null.");
    Preconditions.checkNotNull(change.metadata(), "change.metadata() cannot be null.");

    ChangeKey changeKey = new ChangeKey(change);
    try {
      return this.topicNameCache.get(changeKey, () -> generateTopicName(change));
    } catch (ExecutionException e) {
      throw new DataException("Exception thrown while building schemas.", e);
    }
  }


}
