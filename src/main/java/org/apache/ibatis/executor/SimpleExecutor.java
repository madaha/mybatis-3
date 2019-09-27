/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.executor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Clinton Begin
 */
public class SimpleExecutor extends BaseExecutor {
  private static Logger logger = LoggerFactory.getLogger(SimpleExecutor.class);
  public SimpleExecutor(Configuration configuration, Transaction transaction) {
    super(configuration, transaction);
  }

  @Override
  public int doUpdate(MappedStatement ms, Object parameter) throws SQLException {
    Statement stmt = null;
    try {
      Configuration configuration = ms.getConfiguration();
      StatementHandler handler = configuration.newStatementHandler(this, ms, parameter, RowBounds.DEFAULT, null, null);
      stmt = prepareStatement(handler, ms.getStatementLog());
      return handler.update(stmt);
    } finally {
      closeStatement(stmt);
    }
  }

  @Override
  public <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) throws SQLException {
    Statement stmt = null;
    try {
      long startTime = System.currentTimeMillis();
      Configuration configuration = ms.getConfiguration();
      long endTime = System.currentTimeMillis();
      logger.info("simpleExecutor getConfiguration time = {}", endTime - startTime);

      startTime = System.currentTimeMillis();
      StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, resultHandler, boundSql);
      endTime = System.currentTimeMillis();
      logger.info("simpleExecutor newStatementHandler time = {}", endTime - startTime);

      startTime = System.currentTimeMillis();
      stmt = prepareStatement(handler, ms.getStatementLog());
      endTime = System.currentTimeMillis();
      logger.info("simpleExecutor prepareStatement time = {}", endTime - startTime);

      startTime = System.currentTimeMillis();
      List<E> list = handler.query(stmt, resultHandler);
      endTime = System.currentTimeMillis();
      logger.info("simpleExecutor query time = {}", endTime - startTime);
      return list;
    } finally {
      closeStatement(stmt);
    }
  }

  @Override
  protected <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds, BoundSql boundSql) throws SQLException {
    Configuration configuration = ms.getConfiguration();
    StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, null, boundSql);
    Statement stmt = prepareStatement(handler, ms.getStatementLog());
    Cursor<E> cursor = handler.queryCursor(stmt);
    stmt.closeOnCompletion();
    return cursor;
  }

  @Override
  public List<BatchResult> doFlushStatements(boolean isRollback) {
    return Collections.emptyList();
  }

  private Statement prepareStatement(StatementHandler handler, Log statementLog) throws SQLException {
    Statement stmt;
    long startTime = System.currentTimeMillis();
    Connection connection = getConnection(statementLog);
    long endTime = System.currentTimeMillis();
    logger.info("prepareStatement getConnection time = {}", endTime - startTime);

    startTime = System.currentTimeMillis();
    stmt = handler.prepare(connection, transaction.getTimeout());
    endTime = System.currentTimeMillis();
    logger.info("prepareStatement handler.prepare time = {}", endTime - startTime);

    startTime = System.currentTimeMillis();
    handler.parameterize(stmt);
    endTime = System.currentTimeMillis();
    logger.info("prepareStatement handler.parameterize time = {}", endTime - startTime);
    return stmt;
  }

}
