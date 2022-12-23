package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate p;
    private OpIterator child;
    //child方法参照OpIterator接口
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.p=p;
        this.child=child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }


    /**
     * OpIterator is the iterator interface that all SimpleDB operators should
     * implement. If the iterator is not open, none of the methods should work,
     * and should throw an IllegalStateException.  In addition to any
     * resource allocation/deallocation, an open method should call any
     * child iterator open methods, and in a close method, an iterator
     * should call its children's close methods.
     */
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        //打开时要先打开下层迭代器

        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        //关闭时要先关上层
        super.close();
        child.close();

    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while(child.hasNext()){
            Tuple tuple=child.next();
            if(p.filter(tuple)){
                return tuple;
            }
        }
        return null;
    }


    /**
     * @return return the children DbIterators of this operator. If there is
     *         only one child, return an array of only one element. For join
     *         operators, the order of the children is not important. But they
     *         should be consistent among multiple calls.
     * */
    //参照父类Operator
    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] o=new OpIterator[1];
        o[0]=child;
        return o;
    }

    /**
     * Set the children(child) of this operator. If the operator has only one
     * child, children[0] should be used. If the operator is a join, children[0]
     * and children[1] should be used.
     *
     *
     * @param children
     *            the DbIterators which are to be set as the children(child) of
     *            this operator
     * */
    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child=children[0];
    }

}
