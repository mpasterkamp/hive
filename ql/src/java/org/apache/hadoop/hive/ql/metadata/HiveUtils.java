/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezContext;
import org.apache.hadoop.hive.ql.security.HadoopDefaultAuthenticator;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveMetastoreAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * General collection of helper functions.
 *
 */
public final class HiveUtils {

  public static String escapeString(String str) {
    int length = str.length();
    StringBuilder escape = new StringBuilder(length + 16);

    for (int i = 0; i < length; ++i) {
      char c = str.charAt(i);
      switch (c) {
      case '"':
      case '\\':
        escape.append('\\');
        escape.append(c);
        break;
      case '\b':
        escape.append('\\');
        escape.append('b');
        break;
      case '\f':
        escape.append('\\');
        escape.append('f');
        break;
      case '\n':
        escape.append('\\');
        escape.append('n');
        break;
      case '\r':
        escape.append('\\');
        escape.append('r');
        break;
      case '\t':
        escape.append('\\');
        escape.append('t');
        break;
      default:
        // Control characeters! According to JSON RFC u0020
        if (c < ' ') {
          String hex = Integer.toHexString(c);
          escape.append('\\');
          escape.append('u');
          for (int j = 4; j > hex.length(); --j) {
            escape.append('0');
          }
          escape.append(hex);
        } else {
          escape.append(c);
        }
        break;
      }
    }
    return (escape.toString());
  }

  static final byte[] escapeEscapeBytes = "\\\\".getBytes();;
  static final byte[] escapeUnescapeBytes = "\\".getBytes();
  static final byte[] newLineEscapeBytes = "\\n".getBytes();;
  static final byte[] newLineUnescapeBytes = "\n".getBytes();
  static final byte[] carriageReturnEscapeBytes = "\\r".getBytes();;
  static final byte[] carriageReturnUnescapeBytes = "\r".getBytes();
  static final byte[] tabEscapeBytes = "\\t".getBytes();;
  static final byte[] tabUnescapeBytes = "\t".getBytes();
  static final byte[] ctrlABytes = "\u0001".getBytes();


  public static final Logger LOG = LoggerFactory.getLogger(HiveUtils.class);


  public static Text escapeText(Text text) {
    int length = text.getLength();
    byte[] textBytes = text.getBytes();

    Text escape = new Text(text);
    escape.clear();

    for (int i = 0; i < length; ++i) {
      int c = text.charAt(i);
      byte[] escaped;
      int start;
      int len;

      switch (c) {

      case '\\':
        escaped = escapeEscapeBytes;
        start = 0;
        len = escaped.length;
        break;

      case '\n':
        escaped = newLineEscapeBytes;
        start = 0;
        len = escaped.length;
        break;

      case '\r':
        escaped = carriageReturnEscapeBytes;
        start = 0;
        len = escaped.length;
        break;

      case '\t':
        escaped = tabEscapeBytes;
        start = 0;
        len = escaped.length;
        break;

      case '\u0001':
        escaped = tabUnescapeBytes;
        start = 0;
        len = escaped.length;
        break;

      default:
        escaped = textBytes;
        start = i;
        len = 1;
        break;
      }

      escape.append(escaped, start, len);

    }
    return escape;
  }

  public static int unescapeText(Text text) {
    Text escape = new Text(text);
    text.clear();

    int length = escape.getLength();
    byte[] textBytes = escape.getBytes();

    boolean hadSlash = false;
    for (int i = 0; i < length; ++i) {
      int c = escape.charAt(i);
      switch (c) {
      case '\\':
        if (hadSlash) {
          text.append(textBytes, i, 1);
          hadSlash = false;
        }
        else {
          hadSlash = true;
        }
        break;
      case 'n':
        if (hadSlash) {
          byte[] newLine = newLineUnescapeBytes;
          text.append(newLine, 0, newLine.length);
        }
        else {
          text.append(textBytes, i, 1);
        }
        hadSlash = false;
        break;
      case 'r':
        if (hadSlash) {
          byte[] carriageReturn = carriageReturnUnescapeBytes;
          text.append(carriageReturn, 0, carriageReturn.length);
        }
        else {
          text.append(textBytes, i, 1);
        }
        hadSlash = false;
        break;

      case 't':
        if (hadSlash) {
          byte[] tab = tabUnescapeBytes;
          text.append(tab, 0, tab.length);
        }
        else {
          text.append(textBytes, i, 1);
        }
        hadSlash = false;
        break;

      case '\t':
        if (hadSlash) {
          text.append(textBytes, i-1, 1);
          hadSlash = false;
        }

        byte[] ctrlA = ctrlABytes;
        text.append(ctrlA, 0, ctrlA.length);
        break;

      default:
        if (hadSlash) {
          text.append(textBytes, i-1, 1);
          hadSlash = false;
        }

        text.append(textBytes, i, 1);
        break;
      }
    }
    return text.getLength();
  }

  public static String lightEscapeString(String str) {
    int length = str.length();
    StringBuilder escape = new StringBuilder(length + 16);

    for (int i = 0; i < length; ++i) {
      char c = str.charAt(i);
      switch (c) {
      case '\n':
        escape.append('\\');
        escape.append('n');
        break;
      case '\r':
        escape.append('\\');
        escape.append('r');
        break;
      case '\t':
        escape.append('\\');
        escape.append('t');
        break;
      default:
        escape.append(c);
        break;
      }
    }
    return (escape.toString());
  }

  /**
   * Regenerate an identifier as part of unparsing it back to SQL text.
   */
  public static String unparseIdentifier(String identifier) {
    return unparseIdentifier(identifier, null);
  }

  public static String unparseIdentifier(String identifier, Configuration conf) {
    // In the future, if we support arbitrary characters in
    // identifiers, then we'll need to escape any backticks
    // in identifier by doubling them up.

    // the time has come
    String qIdSupport = conf == null ? null :
      HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT);
    if ( qIdSupport != null && !"none".equals(qIdSupport) ) {
      identifier = identifier.replaceAll("`", "``");
    }
    return "`" + identifier + "`";
  }

  public static HiveStorageHandler getStorageHandler(
    Configuration conf, String className) throws HiveException {

    if (className == null) {
      return null;
    }
    try {
      Class<? extends HiveStorageHandler> handlerClass =
        (Class<? extends HiveStorageHandler>)
        Class.forName(className, true, Utilities.getSessionSpecifiedClassLoader());
      HiveStorageHandler storageHandler = ReflectionUtils.newInstance(handlerClass, conf);
      return storageHandler;
    } catch (ClassNotFoundException e) {
      throw new HiveException("Error in loading storage handler."
          + e.getMessage(), e);
    }
  }

  private HiveUtils() {
    // prevent instantiation
  }

  @SuppressWarnings("unchecked")
  public static List<HiveMetastoreAuthorizationProvider> getMetaStoreAuthorizeProviderManagers(
      Configuration conf, HiveConf.ConfVars authorizationProviderConfKey,
      HiveAuthenticationProvider authenticator) throws HiveException {

    String clsStrs = HiveConf.getVar(conf, authorizationProviderConfKey);
    if(clsStrs == null){
      return null;
    }
    List<HiveMetastoreAuthorizationProvider> authProviders = new ArrayList<HiveMetastoreAuthorizationProvider>();
    for (String clsStr : clsStrs.trim().split(",")) {
      LOG.info("Adding metastore authorization provider: " + clsStr);
      authProviders.add((HiveMetastoreAuthorizationProvider) getAuthorizeProviderManager(conf,
          clsStr, authenticator, false));
    }
    return authProviders;
  }

  /**
   * Create a new instance of HiveAuthorizationProvider
   * @param conf
   * @param authzClassName - authorization provider class name
   * @param authenticator
   * @param nullIfOtherClass - return null if configuration
   *  does not point to a HiveAuthorizationProvider subclass
   * @return new instance of HiveAuthorizationProvider
   * @throws HiveException
   */
  @SuppressWarnings("unchecked")
  public static HiveAuthorizationProvider getAuthorizeProviderManager(
      Configuration conf, String authzClassName,
      HiveAuthenticationProvider authenticator, boolean nullIfOtherClass) throws HiveException {

    HiveAuthorizationProvider ret = null;
    try {
      Class<? extends HiveAuthorizationProvider> cls = null;
      if (authzClassName == null || authzClassName.trim().equals("")) {
        cls = DefaultHiveAuthorizationProvider.class;
      } else {
        Class<?> configClass = Class.forName(authzClassName, true, JavaUtils.getClassLoader());
        if(nullIfOtherClass && !HiveAuthorizationProvider.class.isAssignableFrom(configClass) ){
          return null;
        }
        cls = (Class<? extends HiveAuthorizationProvider>)configClass;
      }
      if (cls != null) {
        ret = ReflectionUtils.newInstance(cls, conf);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    ret.setAuthenticator(authenticator);
    return ret;
  }


  /**
   * Return HiveAuthorizerFactory used by new authorization plugin interface.
   * @param conf
   * @param authorizationProviderConfKey
   * @return
   * @throws HiveException if HiveAuthorizerFactory specified in configuration could not
   */
  public static HiveAuthorizerFactory getAuthorizerFactory(
      Configuration conf, HiveConf.ConfVars authorizationProviderConfKey)
          throws HiveException {

    Class<? extends HiveAuthorizerFactory> cls = conf.getClass(authorizationProviderConfKey.varname,
        SQLStdHiveAuthorizerFactory.class, HiveAuthorizerFactory.class);

    if(cls == null){
      //should not happen as default value is set
      throw new HiveException("Configuration value " + authorizationProviderConfKey.varname
          + " is not set to valid HiveAuthorizerFactory subclass" );
    }

    HiveAuthorizerFactory authFactory = ReflectionUtils.newInstance(cls, conf);
    return authFactory;
  }

  @SuppressWarnings("unchecked")
  public static HiveAuthenticationProvider getAuthenticator(
      Configuration conf, HiveConf.ConfVars authenticatorConfKey
      ) throws HiveException {

    String clsStr = HiveConf.getVar(conf, authenticatorConfKey);

    HiveAuthenticationProvider ret = null;
    try {
      Class<? extends HiveAuthenticationProvider> cls = null;
      if (clsStr == null || clsStr.trim().equals("")) {
        cls = HadoopDefaultAuthenticator.class;
      } else {
        cls = (Class<? extends HiveAuthenticationProvider>) Class.forName(
            clsStr, true, JavaUtils.getClassLoader());
      }
      if (cls != null) {
        ret = ReflectionUtils.newInstance(cls, conf);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return ret;
  }

  public static String getLocalDirList(Configuration conf) {
    if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      TezContext tezContext = (TezContext) TezContext.get();
      if (tezContext != null && tezContext.getTezProcessorContext() != null) {
        return StringUtils.arrayToString(tezContext.getTezProcessorContext().getWorkDirs());
      } // otherwise fall back to return null, i.e. to use local tmp dir only
    }

    return null;
  }

  public static String getReplPolicy(String dbName, String tableName) {
    if ((dbName == null) || (dbName.isEmpty())) {
      return "*.*";
    } else if ((tableName == null) || (tableName.isEmpty())) {
      return dbName.toLowerCase() + ".*";
    } else {
      return dbName.toLowerCase() + "." + tableName.toLowerCase();
    }
  }

  public static Path getDumpPath(Path root, String dbName, String tableName) {
    assert (dbName != null);
    if ((tableName != null) && (!tableName.isEmpty())) {
      return new Path(root, dbName + "." + tableName);
    }
    return new Path(root, dbName);
  }

  public static String toStringRepresentation(RelNode node) {
    StringBuilder sb = new StringBuilder();
    sb.append(node.getRelTypeName());
    sb.append("(");
    String name = node.getClass().getSimpleName();
    RelExpression exp = RelExpression.valueOf(node.getClass().getSimpleName());
    String conditionString = "";
    switch(exp) {
      case HiveTableScan:
        HiveTableScan tableScan = (HiveTableScan) node;
        sb.append(toStringRepresentation(tableScan.getInputs()));
        sb.append(tableScan.getTableAlias());
        break;
      case HiveFilter:
        HiveFilter filter = (HiveFilter) node;
        sb.append(toStringRepresentation(filter.getInput()));
        sb.append(", ");
        conditionString = rexToString(filter.getCondition(), filter);
        sb.append(conditionString);
        break;
      case HiveJoin:
        HiveJoin join = (HiveJoin) node;
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        String leftString = toStringRepresentation(left);
        String rightString = toStringRepresentation(right);
        conditionString = rexToString(join.getCondition(), join);
        boolean reorder = leftString.compareTo(rightString) > 0;
        if (reorder) {
          String tempString = leftString;
          leftString = rightString;
          rightString = tempString;
        }
        sb.append(leftString);
        sb.append(", ");
        sb.append(rightString);
        sb.append(", ");
        sb.append(conditionString);
        break;
      case HiveProject:
        HiveProject project = (HiveProject) node;
        String columns = String.join(", ", project.getRowType().getFieldNames());
        sb.append(toStringRepresentation(project.getInputs()));
        sb.append(", ");
        sb.append(String.join(", ", columns));
        break;
      case HiveAggregate:
        HiveAggregate aggregate = (HiveAggregate) node;
        List<String> fieldNames = aggregate.getInput().getRowType().getFieldNames();
        sb.append(toStringRepresentation(aggregate.getInputs()));
        sb.append(", (");
        for (AggregateCall call : aggregate.getAggCallList()) {
          sb.append(aggCallToString(call, fieldNames));
          sb.append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append("), (");
        // Parse the tostring of the groupset
        String groupset = aggregate.getGroupSet().toString();
        // In case of empty groupset
        if (!groupset.equals("{}")) {
          groupset = groupset.substring(1, groupset.length() - 1);
          String[] columnIndexStrings = groupset.split(", ");
          for (String s : columnIndexStrings) {
            sb.append(fieldNames.get(Integer.parseInt(s)));
            sb.append(", ");
          }
          sb.delete(sb.length() - 2, sb.length());
        }
        sb.append(")");
    }
    sb.append(")");
    return sb.toString();
  }

  private static String toStringRepresentation(List<RelNode> nodes) {
    if (nodes.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (RelNode node : nodes) {
      sb.append(toStringRepresentation(node));
      sb.append(", ");
    }
    return sb.substring(0, sb.length() - 2);
  }


  public static String aggCallToString(AggregateCall call, List<String> fieldNames) {
    StringBuilder buf = new StringBuilder(call.getAggregation().getName());
    buf.append("(");
    if (call.isDistinct()) {
      buf.append((call.getArgList().size() == 0) ? "DISTINCT" : "DISTINCT ");
    }
    int i = -1;
    for (Integer arg : call.getArgList()) {
      if (++i > 0) {
        buf.append(", ");
      }
      buf.append(fieldNames.get(arg));
    }
    buf.append(")");
    if (call.hasFilter()) {
      buf.append(" FILTER $");
      buf.append(call.filterArg);
    }
    return buf.toString();
  }

  enum RelExpression {
    HiveTableScan,
    HiveFilter,
    HiveJoin,
    HiveProject,
    HiveAggregate
  }

  public static String rexToString(RexNode rexNode, RelNode relNode) {
    StringBuilder builder = new StringBuilder();
    String className = rexNode.getClass().getSimpleName();
    RexExpression expression = RexExpression.valueOf(className);
    switch (expression) {
      case RexCall:
        RexCall call = (RexCall) rexNode;
        String condition = call.op.toString();
        String operands = call.operands.stream().map(x -> rexToString(x, relNode)).collect(Collectors.joining(", "));
        builder.append(condition);
        builder.append("(");
        builder.append(operands);
        builder.append(")");
        break;
      case RexFieldAccess:
        throw new NotImplementedException();
//        break;
      case RexLiteral:
        RexLiteral lit = (RexLiteral) rexNode;
        builder.append(lit.toString());
        break;
      case RexVariable:
        throw new NotImplementedException();
//        break;
      case RexRangeRef:
        throw new NotImplementedException();
//        break;
      case RexInputRef:
        RexInputRef ref = (RexInputRef) rexNode;
        String column = getReferenceColumn(ref, relNode);
        builder.append(column);
        break;
      default:
        throw new NotImplementedException();

    }
    return builder.toString();
  }

  enum RexExpression {
    RexCall,
    RexFieldAccess,
    RexLiteral,
    RexVariable,
    RexRangeRef,
    RexInputRef
  }

  private static String getReferenceColumn(RexInputRef ref, RelNode node) {
    int index = getReferenceIndex(ref);
    String column = node.getRowType().getFieldList().get(index).getName();
    return column;
  }

  private static int getReferenceIndex(RexInputRef ref) {
    String originalIndexString = ref.toString().substring(1);
    int index = Integer.parseInt(originalIndexString);
    return index;
  }
}
