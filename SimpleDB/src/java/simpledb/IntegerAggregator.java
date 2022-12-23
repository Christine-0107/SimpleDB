package simpledb;

import java.lang.reflect.Array;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private abstract class AggHandler{
        HashMap<Field,Integer> aggResult;
        abstract void handle(Field gbField, IntField aggField);
        public AggHandler(){
            aggResult = new HashMap<>();
        }
        public HashMap<Field,Integer> getAggResult(){
            return aggResult;
        }
    }

    private class MinHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, Math.min(aggResult.get(gbField) , value));
            }
            else {
                aggResult.put(gbField, value);
            }
        }
    }
    private class MaxHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, Math.max(aggResult.get(gbField),value));
            }
            else {
                aggResult.put(gbField,value);
            }
        }
    }

    private class CountHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, aggResult.get(gbField) + 1);
            }
            else {
                aggResult.put(gbField, 1);
            }
        }
    }

    private class SumHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, aggResult.get(gbField) + value);
            }
            else {
                aggResult.put(gbField, value);
            }
        }
    }

    private class AvgHandler extends  AggHandler{
        HashMap<Field, Integer> sum;
        HashMap<Field, Integer> count;
        private AvgHandler(){
            sum = new HashMap<>();
            count = new HashMap<>();
        }
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(sum.containsKey(gbField) && count.containsKey(gbField)){
                sum.put(gbField, sum.get(gbField) + value);
                count.put(gbField, count.get(gbField) + 1);
            }
            else {
                sum.put(gbField, value);
                count.put(gbField, 1);
            }
            int avg = sum.get(gbField) / count.get(gbField);
            aggResult.put(gbField, avg);
        }
    }


    private int gbFieldIndex;
    private Type gbFieldType;
    private int aggFieldIndex;
    private AggHandler aggHandler;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     *            group-by字段的从0开始的索引
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     *            group-by字段的类型
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     *            聚合字段从0开始的索引
     * @param what
     *            the aggregation operator
     *            聚合操作符
     */
    //Op是在Aggregator中定义的枚举类型，包括MIN, MAX, SUM, AVG, COUNT,（SUM_COUNT,SC_AVG）
    //本次实验只用到前5个
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFieldIndex=gbfield;
        this.gbFieldType=gbfieldtype;
        this.aggFieldIndex=afield;
        switch (what) {
            case MIN:
                aggHandler = new MinHandler();
                break;
            case MAX:
                aggHandler = new MaxHandler();
                break;
            case SUM:
                aggHandler = new SumHandler();
                break;
            case COUNT:
                aggHandler = new CountHandler();
                break;
            case AVG:
                aggHandler = new AvgHandler();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation operator ");
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    //聚合字段是整型。聚合字段和分组字段各一个且可以通过给定的构造函数中给出的字段号从元组中定位
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //根据tup和字段号确定字段
        Field gbField;
        IntField aggField = (IntField) tup.getField(aggFieldIndex);
        if(gbFieldIndex == NO_GROUPING){
            gbField = null;
        }
        else {
            gbField = tup.getField(gbFieldIndex);
        }
        aggHandler.handle(gbField,aggField);
    }




    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    //为聚合操作得到的元组添加迭代器
    public OpIterator iterator() {
        // some code goes here
        //利用TupleIterator类，需要最终的元组模式和元组列表
        HashMap<Field, Integer> result = aggHandler.getAggResult();
        Type[] fieldTypes;
        String[] fieldNames;
        TupleDesc tupleDesc;
        ArrayList<Tuple> tuples = new ArrayList<>();
        //根据有无分组属性分为两种情况
        if (gbFieldIndex == NO_GROUPING) { //无分组属性时模式中只有一个INT类型
            fieldTypes = new Type[]{Type.INT_TYPE};
            fieldNames = new String[]{"aggregateValue"};
            tupleDesc = new TupleDesc(fieldTypes, fieldNames);
            Tuple tuple = new Tuple(tupleDesc);
            //结果的hashMap中键为null对应的值是要添加的
            IntField resultField = new IntField(result.get(null));
            tuple.setField(0, resultField);
            tuples.add(tuple);
        }
        else { //有分组属性时模式中有一个分组类型和一个INT类型
            fieldTypes = new Type[]{gbFieldType, Type.INT_TYPE};
            fieldNames = new String[]{"groupByValue", "aggregateValue"};
            tupleDesc = new TupleDesc(fieldTypes, fieldNames);
            for (Field field : result.keySet()) {
                Tuple tuple = new Tuple(tupleDesc);
                if (gbFieldType == Type.INT_TYPE) {
                    IntField gbField = (IntField) field;
                    tuple.setField(0, gbField);
                }
                else {
                    StringField gbField = (StringField) field;
                    tuple.setField(0, gbField);
                }
                IntField resultField = new IntField(result.get(field));
                tuple.setField(1, resultField);
                tuples.add(tuple);
            }
        }

        return new TupleIterator(tupleDesc, tuples);
    }


}
