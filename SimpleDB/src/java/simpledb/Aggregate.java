package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int aggFieldIndex;
    private int gbFieldIndex;
    private Aggregator.Op aop;
    private TupleDesc tupleDesc;
    private Aggregator aggregator;
    private OpIterator aggIterator;


    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     *            需要聚合的元组，通过迭代器访问
     * @param afield
     *            The column over which we are computing an aggregate
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     *            运算符
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child=child;
        this.aggFieldIndex=afield;
        this.gbFieldIndex=gfield;
        this.aop=aop;

        Type gbFieldType;
        if(gbFieldIndex==Aggregator.NO_GROUPING){
            gbFieldType=null;
        }
        else{
            gbFieldType=child.getTupleDesc().getFieldType(gbFieldIndex);
        }
        Type aggFieldType=child.getTupleDesc().getFieldType(aggFieldIndex);
        Type[] fieldType;
        String[] fieldName;
        String aggFieldName=child.getTupleDesc().getFieldName(aggFieldIndex);
        if(gbFieldType==null){
            fieldType=new Type[]{aggFieldType};
            fieldName=new String[]{aggFieldName};
        }
        else{
            String gbFieldName=child.getTupleDesc().getFieldName(gbFieldIndex);
            fieldType=new Type[]{gbFieldType,aggFieldType};
            fieldName=new String[]{gbFieldName,aggFieldName};
        }
        tupleDesc=new TupleDesc(fieldType,fieldName);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
        return this.gbFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if(gbFieldIndex==Aggregator.NO_GROUPING){
            return null;
        }
        else{
            return tupleDesc.getFieldName(0);
        }
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	    return this.aggFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	    if(gbFieldIndex==Aggregator.NO_GROUPING){
	        return tupleDesc.getFieldName(0);
        }
	    else{
	        return tupleDesc.getFieldName(1);
        }
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	    return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        child.open();
        Type gbFieldType;
        if(gbFieldIndex==Aggregator.NO_GROUPING){
            gbFieldType=null;
        }
        else{
            gbFieldType=child.getTupleDesc().getFieldType(gbFieldIndex);
        }
        Type aggFieldType=child.getTupleDesc().getFieldType(aggFieldIndex);
        if(aggFieldType==Type.INT_TYPE){
            aggregator=new IntegerAggregator(gbFieldIndex,gbFieldType,aggFieldIndex,aop);
        }
        else{
            aggregator=new StringAggregator(gbFieldIndex,gbFieldType,aggFieldIndex,aop);
        }
        while(child.hasNext()){
            aggregator.mergeTupleIntoGroup(child.next());
        }
        aggIterator=aggregator.iterator();
        aggIterator.open();
        super.open();

    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if(aggIterator.hasNext()){
            return aggIterator.next();
        }
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        child.rewind();
        aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	    return tupleDesc;
    }

    public void close() {
	// some code goes here
        super.close();
        child.close();
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
