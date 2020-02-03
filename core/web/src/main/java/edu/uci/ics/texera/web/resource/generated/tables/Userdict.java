/*
 * This file is generated by jOOQ.
 */
package edu.uci.ics.texera.web.resource.generated.tables;


import edu.uci.ics.texera.web.resource.generated.Indexes;
import edu.uci.ics.texera.web.resource.generated.Keys;
import edu.uci.ics.texera.web.resource.generated.Texera;
import edu.uci.ics.texera.web.resource.generated.tables.records.UserdictRecord;

import java.util.Arrays;
import java.util.List;

import javax.annotation.processing.Generated;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Identity;
import org.jooq.Index;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Row3;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;


/**
 * This class is generated by jOOQ.
 */
@Generated(
    value = {
        "http://www.jooq.org",
        "jOOQ version:3.12.4"
    },
    comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Userdict extends TableImpl<UserdictRecord> {

    private static final long serialVersionUID = -983192478;

    /**
     * The reference instance of <code>texera.userdict</code>
     */
    public static final Userdict USERDICT = new Userdict();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<UserdictRecord> getRecordType() {
        return UserdictRecord.class;
    }

    /**
     * The column <code>texera.userdict.dictID</code>.
     */
    public final TableField<UserdictRecord, Integer> DICTID = createField(DSL.name("dictID"), org.jooq.impl.SQLDataType.INTEGER.nullable(false).identity(true), this, "");

    /**
     * The column <code>texera.userdict.dictName</code>.
     */
    public final TableField<UserdictRecord, String> DICTNAME = createField(DSL.name("dictName"), org.jooq.impl.SQLDataType.VARCHAR(32).nullable(false), this, "");

    /**
     * The column <code>texera.userdict.dictContent</code>.
     */
    public final TableField<UserdictRecord, byte[]> DICTCONTENT = createField(DSL.name("dictContent"), org.jooq.impl.SQLDataType.BLOB.nullable(false), this, "");

    /**
     * Create a <code>texera.userdict</code> table reference
     */
    public Userdict() {
        this(DSL.name("userdict"), null);
    }

    /**
     * Create an aliased <code>texera.userdict</code> table reference
     */
    public Userdict(String alias) {
        this(DSL.name(alias), USERDICT);
    }

    /**
     * Create an aliased <code>texera.userdict</code> table reference
     */
    public Userdict(Name alias) {
        this(alias, USERDICT);
    }

    private Userdict(Name alias, Table<UserdictRecord> aliased) {
        this(alias, aliased, null);
    }

    private Userdict(Name alias, Table<UserdictRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""));
    }

    public <O extends Record> Userdict(Table<O> child, ForeignKey<O, UserdictRecord> key) {
        super(child, key, USERDICT);
    }

    @Override
    public Schema getSchema() {
        return Texera.TEXERA;
    }

    @Override
    public List<Index> getIndexes() {
        return Arrays.<Index>asList(Indexes.USERDICT_DICTID, Indexes.USERDICT_PRIMARY);
    }

    @Override
    public Identity<UserdictRecord, Integer> getIdentity() {
        return Keys.IDENTITY_USERDICT;
    }

    @Override
    public UniqueKey<UserdictRecord> getPrimaryKey() {
        return Keys.KEY_USERDICT_PRIMARY;
    }

    @Override
    public List<UniqueKey<UserdictRecord>> getKeys() {
        return Arrays.<UniqueKey<UserdictRecord>>asList(Keys.KEY_USERDICT_PRIMARY);
    }

    @Override
    public Userdict as(String alias) {
        return new Userdict(DSL.name(alias), this);
    }

    @Override
    public Userdict as(Name alias) {
        return new Userdict(alias, this);
    }

    /**
     * Rename this table
     */
    @Override
    public Userdict rename(String name) {
        return new Userdict(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public Userdict rename(Name name) {
        return new Userdict(name, null);
    }

    // -------------------------------------------------------------------------
    // Row3 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row3<Integer, String, byte[]> fieldsRow() {
        return (Row3) super.fieldsRow();
    }
}