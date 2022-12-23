package simpledb;

import java.nio.Buffer;
import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    //lab2
    /***
     * 访问通过BufferPool进行
     * 插入新元组的过程为：
     * 1.BufferPool调用HeapFile的insertTuple方法
     * 2.HeapFile调用BufferPool的getPage方法。若该表的页在BufferPool中，直接返回；
     *   若不在，则BufferPool调用HeapFile的readPage方法从磁盘上读取页。
     * 3.找到有空slot的页，调用HeapPage的insertTuple方法插入；并将插入后的页返回给BufferPool
     * 4.此时BufferPool中的页和磁盘上的页是不同的，该页被标记为dirty
     * 5.BufferPool未满就保存，满了就置换一页
     * 6.置换时从BufferPool移除的若为脏页，则调用HeapFile的writePage将其写回磁盘
     *
     * 删除元组同理
     */
    boolean dirty; //标志是否为脏页
    TransactionId dirtyId; //产生脏页的事务id

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    //通过header说明页中的slots和slots的数量
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
     @return the number of tuples on this page
     */
    private int getNumTuples() {
        // some code goes here
        //页大小/(元组大小+header位)
        int tupleNum=(int)Math.floor((BufferPool.getPageSize()*8*1.0)/(td.getSize()*8+1));
        return tupleNum;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {

        // some code goes here
        //元组数/8位
        int headerSize=(int)Math.ceil((getNumTuples()*1.0)/8);
        return headerSize;

    }

    /** Return a view of this page before it was modified
     -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        RecordId recordId=t.getRecordId(); //获取id
        for (int i = 0; i < numSlots; i++) {
            if (isSlotUsed(i) && recordId.equals(tuples[i].getRecordId())) {
                markSlotUsed(i, false);
                tuples[i] = null;
                return;
            }
        }
        throw new DbException("No matched tuple");
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if(getNumEmptySlots()==0){
            throw new DbException("This page is full!");
        }
        if(!t.getTupleDesc().equals(td)){
            throw new DbException("TupleDesc mismatched");
        }
        for(int i=0;i<numSlots;i++){
            if(!isSlotUsed(i)){
                markSlotUsed(i,true);
                t.setRecordId(new RecordId(pid,i)); //为该元组设置RecordId
                tuples[i]=t;
                break;
            }
        }

    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
        this.dirty=dirty;
        this.dirtyId=tid;

    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        if(this.dirty){
            return this.dirtyId;
        }
        return null;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int count=0;
        for(int i=0;i<numSlots;i++){
            if(!isSlotUsed(i)){
                count++;
            }
        }
        return count;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        if (i < numSlots) {
            int now = i/8;
            int offset = i%8;
            return (header[now]&(0x1<<offset))!=0; //从右向左填充
        }
        return false;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    //i对应的是元组编号。value==true时i对应的bit改为1，否则改为0
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        if(i<numSlots){
            int now=i/8;
            int offset=i%8;
            byte bitPos=(byte)(0x1<<offset);
            if(value){
                header[now]|=bitPos;
            }
            else{
                header[now]&=(~bitPos);
            }
        }

    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    protected class HeapPageTupleIterator implements Iterator{ //为了实现对于remove的要求
        private final Iterator<Tuple> tupleIterator;
        public HeapPageTupleIterator(){
            ArrayList<Tuple> tupleArrayList=new ArrayList<>();
            for(int i=0;i<numSlots;i++){
                if(isSlotUsed(i)){
                    tupleArrayList.add(tuples[i]);
                }
            }
            tupleIterator=tupleArrayList.iterator();
        }
        @Override
        public void remove(){
            throw new UnsupportedOperationException("TupleIterator: remove not supported");
        }


        @Override
        public boolean hasNext(){
            return tupleIterator.hasNext();
        }
        @Override
        public Object next(){
            return tupleIterator.next();
        }

    }


    public Iterator<Tuple> iterator() {
        // some code goes here
        return new HeapPageTupleIterator();
    }

}

