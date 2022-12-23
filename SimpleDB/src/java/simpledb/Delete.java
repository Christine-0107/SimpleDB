package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;

    private TupleDesc tupleDesc;
    private int count;
    boolean isDeleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid=t;
        this.child=child;
        this.count=-1;
        this.isDeleted=false;
        Type[] fieldType=new Type[]{Type.INT_TYPE};
        this.tupleDesc=new TupleDesc(fieldType);

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.count=0;
        this.child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.count=-1;
        this.isDeleted=false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.count=0;
        this.isDeleted=false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(this.isDeleted){
            return null;
        }
        this.isDeleted=true;
        while(this.child.hasNext()){
            try {
                Database.getBufferPool().deleteTuple(this.tid, child.next());
                this.count++;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
        Tuple tuple=new Tuple(this.tupleDesc);
        tuple.setField(0,new IntField(this.count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child=children[0];
    }

}
