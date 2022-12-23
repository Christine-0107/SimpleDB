package simpledb;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
//将元组与指定域的值比较（包括字段号，比较运算符，字段内容）
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * 通过整数值访问操作的接口，以方便命令行。
         *
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    private final int field;
    private final Op op;
    private final Field operand;
    
    /**
     * Constructor.
     * 
     * @param field
     *            field number of passed in tuples to compare against.
     *            传入元组的要比较的字段数
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     *            传入元组的要比较的字段值
     *
     */
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
        this.field=field;
        this.op=op;
        this.operand=operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        // some code goes here
        return field;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        // some code goes here
        return op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand()
    {
        // some code goes here
        return operand;
    }
    
    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    //比较t中构造函数字段编号中的内容与构造函数指定字段数内容，用op比较。
    public boolean filter(Tuple t) {
        // some code goes here
        return t.getField(field).compare(op,operand);
        //通过t.getField获取t中field字段内容
        //compare(op,operand)是Field中定义的方法
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
