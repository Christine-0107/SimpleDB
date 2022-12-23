package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private int tableId;
    private String tableAlias;
    private DbFile dbFile;
    private DbFileIterator dbFileIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid=tid;
        this.tableId=tableid;
        this.tableAlias=tableAlias;
        this.dbFile=Database.getCatalog().getDatabaseFile(tableid);
        this.dbFileIterator=dbFile.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId=tableid;
        this.tableAlias=tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    //TupleDesc包括类型和域名，其中对于域名为其添加alias.作为前缀
    public TupleDesc getTupleDesc() {
        // some code goes here
        final TupleDesc tupleDesc=dbFile.getTupleDesc();
        Type[] typeAr=new Type[tupleDesc.numFields()];
        String[] fieldAr=new String[tupleDesc.numFields()];

        String prefix="null";
        if(tableAlias!=null){
            prefix=tableAlias; //表的别名作为前缀
        }

        for(int i=0;i<tupleDesc.numFields();i++){
            typeAr[i]=tupleDesc.getFieldType(i);
            String fieldName=tupleDesc.getFieldName(i);
            if(fieldName==null){
                fieldName="null";
            }
            fieldName=prefix+"."+fieldName;
            fieldAr[i]=fieldName;
        }
        return new TupleDesc(typeAr,fieldAr);

    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(dbFileIterator!=null){
            return dbFileIterator.hasNext();
        }
        throw new TransactionAbortedException();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        Tuple tuple=dbFileIterator.next();
        if(tuple != null){
            return tuple;
        } else {
            throw new NoSuchElementException("This is the last element");
        }
    }

    public void close() {
        // some code goes here
        dbFileIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        dbFileIterator.rewind();
    }
}
