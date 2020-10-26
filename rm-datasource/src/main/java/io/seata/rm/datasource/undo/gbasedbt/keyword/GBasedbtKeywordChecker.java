/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.datasource.undo.gbasedbt.keyword;

import io.seata.common.loader.LoadLevel;
import io.seata.rm.datasource.undo.KeywordChecker;
import io.seata.sqlparser.util.JdbcConstants;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The type gbase8s sql keyword checker.
 *
 * @author tianmaotalk
 */
@LoadLevel(name = JdbcConstants.GBASEDBT)
public class GBasedbtKeywordChecker implements KeywordChecker {

    private Set<String> keywordSet = Arrays.stream(GBasedbtKeyword.values()).map(GBasedbtKeyword::name).collect(Collectors.toSet());

    /**
     * gbase8s keyword
     */
    private enum GBasedbtKeyword {
        /**
         * ACCESS is gbase8s keyword
         */
        ACCESS("ACCESS"),
        /**
         * ADD is gbase8s keyword
         */
        ADD("ADD"),
        /**
         * ALL is gbase8s keyword
         */
        ALL("ALL"),
        /**
         * ALTER is gbase8s keyword
         */
        ALTER("ALTER"),
        /**
         * AND is gbase8s keyword
         */
        AND("AND"),
        /**
         * ANY is gbase8s keyword
         */
        ANY("ANY"),
        /**
         * AS is gbase8s keyword
         */
        AS("AS"),
        /**
         * ASC is gbase8s keyword
         */
        ASC("ASC"),
        /**
         * AUDIT is gbase8s keyword
         */
        AUDIT("AUDIT"),
        /**
         * BETWEEN is gbase8s keyword
         */
        BETWEEN("BETWEEN"),
        /**
         * BY is gbase8s keyword
         */
        BY("BY"),
        /**
         * CHAR is gbase8s keyword
         */
        CHAR("CHAR"),
        /**
         * CHECK is gbase8s keyword
         */
        CHECK("CHECK"),
        /**
         * CLUSTER is gbase8s keyword
         */
        CLUSTER("CLUSTER"),
        /**
         * COLUMN is gbase8s keyword
         */
        COLUMN("COLUMN"),
        /**
         * COLUMN_VALUE is gbase8s keyword
         */
        COLUMN_VALUE("COLUMN_VALUE"),
        /**
         * COMMENT is gbase8s keyword
         */
        COMMENT("COMMENT"),
        /**
         * COMPRESS is gbase8s keyword
         */
        COMPRESS("COMPRESS"),
        /**
         * CONNECT is gbase8s keyword
         */
        CONNECT("CONNECT"),
        /**
         * CREATE is gbase8s keyword
         */
        CREATE("CREATE"),
        /**
         * CURRENT is gbase8s keyword
         */
        CURRENT("CURRENT"),
        /**
         * DATE is gbase8s keyword
         */
        DATE("DATE"),
        /**
         * DECIMAL is gbase8s keyword
         */
        DECIMAL("DECIMAL"),
        /**
         * DEFAULT is gbase8s keyword
         */
        DEFAULT("DEFAULT"),
        /**
         * DELETE is gbase8s keyword
         */
        DELETE("DELETE"),
        /**
         * DESC is gbase8s keyword
         */
        DESC("DESC"),
        /**
         * DISTINCT is gbase8s keyword
         */
        DISTINCT("DISTINCT"),
        /**
         * DROP is gbase8s keyword
         */
        DROP("DROP"),
        /**
         * ELSE is gbase8s keyword
         */
        ELSE("ELSE"),
        /**
         * EXCLUSIVE is gbase8s keyword
         */
        EXCLUSIVE("EXCLUSIVE"),
        /**
         * EXISTS is gbase8s keyword
         */
        EXISTS("EXISTS"),
        /**
         * FILE is gbase8s keyword
         */
        FILE("FILE"),
        /**
         * FLOAT is gbase8s keyword
         */
        FLOAT("FLOAT"),
        /**
         * FOR is gbase8s keyword
         */
        FOR("FOR"),
        /**
         * FROM is gbase8s keyword
         */
        FROM("FROM"),
        /**
         * GRANT is gbase8s keyword
         */
        GRANT("GRANT"),
        /**
         * GROUP is gbase8s keyword
         */
        GROUP("GROUP"),
        /**
         * HAVING is gbase8s keyword
         */
        HAVING("HAVING"),
        /**
         * IDENTIFIED is gbase8s keyword
         */
        IDENTIFIED("IDENTIFIED"),
        /**
         * IMMEDIATE is gbase8s keyword
         */
        IMMEDIATE("IMMEDIATE"),
        /**
         * IN is gbase8s keyword
         */
        IN("IN"),
        /**
         * INCREMENT is gbase8s keyword
         */
        INCREMENT("INCREMENT"),
        /**
         * INDEX is gbase8s keyword
         */
        INDEX("INDEX"),
        /**
         * INITIAL is gbase8s keyword
         */
        INITIAL("INITIAL"),
        /**
         * INSERT is gbase8s keyword
         */
        INSERT("INSERT"),
        /**
         * INTEGER is gbase8s keyword
         */
        INTEGER("INTEGER"),
        /**
         * INTERSECT is gbase8s keyword
         */
        INTERSECT("INTERSECT"),
        /**
         * INTO is gbase8s keyword
         */
        INTO("INTO"),
        /**
         * IS is gbase8s keyword
         */
        IS("IS"),
        /**
         * LEVEL is gbase8s keyword
         */
        LEVEL("LEVEL"),
        /**
         * LIKE is gbase8s keyword
         */
        LIKE("LIKE"),
        /**
         * LOCK is gbase8s keyword
         */
        LOCK("LOCK"),
        /**
         * LONG is gbase8s keyword
         */
        LONG("LONG"),
        /**
         * MAXEXTENTS is gbase8s keyword
         */
        MAXEXTENTS("MAXEXTENTS"),
        /**
         * MINUS is gbase8s keyword
         */
        MINUS("MINUS"),
        /**
         * MLSLABEL is gbase8s keyword
         */
        MLSLABEL("MLSLABEL"),
        /**
         * MODE is gbase8s keyword
         */
        MODE("MODE"),
        /**
         * MODIFY is gbase8s keyword
         */
        MODIFY("MODIFY"),
        /**
         * NESTED_TABLE_ID is gbase8s keyword
         */
        NESTED_TABLE_ID("NESTED_TABLE_ID"),
        /**
         * NOAUDIT is gbase8s keyword
         */
        NOAUDIT("NOAUDIT"),
        /**
         * NOCOMPRESS is gbase8s keyword
         */
        NOCOMPRESS("NOCOMPRESS"),
        /**
         * NOT is gbase8s keyword
         */
        NOT("NOT"),
        /**
         * NOWAIT is gbase8s keyword
         */
        NOWAIT("NOWAIT"),
        /**
         * NULL is gbase8s keyword
         */
        NULL("NULL"),
        /**
         * NUMBER is gbase8s keyword
         */
        NUMBER("NUMBER"),
        /**
         * OF is gbase8s keyword
         */
        OF("OF"),
        /**
         * OFFLINE is gbase8s keyword
         */
        OFFLINE("OFFLINE"),
        /**
         * ON is gbase8s keyword
         */
        ON("ON"),
        /**
         * ONLINE is gbase8s keyword
         */
        ONLINE("ONLINE"),
        /**
         * OPTION is gbase8s keyword
         */
        OPTION("OPTION"),
        /**
         * OR is gbase8s keyword
         */
        OR("OR"),
        /**
         * ORDER is gbase8s keyword
         */
        ORDER("ORDER"),
        /**
         * PCTFREE is gbase8s keyword
         */
        PCTFREE("PCTFREE"),
        /**
         * PRIOR is gbase8s keyword
         */
        PRIOR("PRIOR"),
        /**
         * PUBLIC is gbase8s keyword
         */
        PUBLIC("PUBLIC"),
        /**
         * RAW is gbase8s keyword
         */
        RAW("RAW"),
        /**
         * RENAME is gbase8s keyword
         */
        RENAME("RENAME"),
        /**
         * RESOURCE is gbase8s keyword
         */
        RESOURCE("RESOURCE"),
        /**
         * REVOKE is gbase8s keyword
         */
        REVOKE("REVOKE"),
        /**
         * ROW is gbase8s keyword
         */
        ROW("ROW"),
        /**
         * ROWID is gbase8s keyword
         */
        ROWID("ROWID"),
        /**
         * ROWNUM is gbase8s keyword
         */
        ROWNUM("ROWNUM"),
        /**
         * ROWS is gbase8s keyword
         */
        ROWS("ROWS"),
        /**
         * SELECT is gbase8s keyword
         */
        SELECT("SELECT"),
        /**
         * SESSION is gbase8s keyword
         */
        SESSION("SESSION"),
        /**
         * SET is gbase8s keyword
         */
        SET("SET"),
        /**
         * SHARE is gbase8s keyword
         */
        SHARE("SHARE"),
        /**
         * SIZE is gbase8s keyword
         */
        SIZE("SIZE"),
        /**
         * SMALLINT is gbase8s keyword
         */
        SMALLINT("SMALLINT"),
        /**
         * START is gbase8s keyword
         */
        START("START"),
        /**
         * SUCCESSFUL is gbase8s keyword
         */
        SUCCESSFUL("SUCCESSFUL"),
        /**
         * SYNONYM is gbase8s keyword
         */
        SYNONYM("SYNONYM"),
        /**
         * SYSDATE is gbase8s keyword
         */
        SYSDATE("SYSDATE"),
        /**
         * TABLE is gbase8s keyword
         */
        TABLE("TABLE"),
        /**
         * THEN is gbase8s keyword
         */
        THEN("THEN"),
        /**
         * TO is gbase8s keyword
         */
        TO("TO"),
        /**
         * TRIGGER is gbase8s keyword
         */
        TRIGGER("TRIGGER"),
        /**
         * UID is gbase8s keyword
         */
        UID("UID"),
        /**
         * UNION is gbase8s keyword
         */
        UNION("UNION"),
        /**
         * UNIQUE is gbase8s keyword
         */
        UNIQUE("UNIQUE"),
        /**
         * UPDATE is gbase8s keyword
         */
        UPDATE("UPDATE"),
        /**
         * USER is gbase8s keyword
         */
        USER("USER"),
        /**
         * VALIDATE is gbase8s keyword
         */
        VALIDATE("VALIDATE"),
        /**
         * VALUES is gbase8s keyword
         */
        VALUES("VALUES"),
        /**
         * VARCHAR is gbase8s keyword
         */
        VARCHAR("VARCHAR"),
        /**
         * VARCHAR is gbase8s keyword
         */
        LVARCHAR("LVARCHAR"),
        /**
         * VARCHAR2 is gbase8s keyword
         */
        VARCHAR2("VARCHAR2"),
        /**
         * VIEW is gbase8s keyword
         */
        VIEW("VIEW"),
        /**
         * WHENEVER is gbase8s keyword
         */
        WHENEVER("WHENEVER"),
        /**
         * WHERE is gbase8s keyword
         */
        WHERE("WHERE"),
        /**
         * WITH is gbase8s keyword
         */
        WITH("WITH");
        /**
         * The Name.
         */
        public final String name;

        GBasedbtKeyword(String name) {
            this.name = name;
        }
    }

    @Override
    public boolean check(String fieldOrTableName) {
        if (keywordSet.contains(fieldOrTableName)) {
            return true;
        }
        if (fieldOrTableName != null) {
            fieldOrTableName = fieldOrTableName.toUpperCase();
        }
        return keywordSet.contains(fieldOrTableName);

    }

    @Override
    public boolean checkEscape(String fieldOrTableName) {
        boolean check = check(fieldOrTableName);
        // gbase8s
        // we are recommend table name and column name must uppercase.
        // if exists full uppercase, the table name or column name does't bundle escape symbol.
        if (!check && isUppercase(fieldOrTableName)) {
            return false;
        }
        return true;
    }

    private static boolean isUppercase(String fieldOrTableName) {
        if (fieldOrTableName == null) {
            return false;
        }
        char[] chars = fieldOrTableName.toCharArray();
        for (char ch : chars) {
            if (ch >= 'a' && ch <= 'z') {
                return false;
            }
        }
        return true;
    }
}
