package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
//DbFile的接口，是由存各个元组的页组成的
public class HeapFile implements DbFile {
    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file=f;
        this.tupleDesc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    //将该HeapFile的绝对路径名生成hashcode作为ID
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    /**
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    public Page readPage(PageId pid) {
        // some code goes here
        //创建一页需要heapPageId和一个字节数组
        //创建一个HeapPageId对象需要tableId和pgNo
        int tableid=pid.getTableId(); //该HeapFile对应的tableid
        int pgNo=pid.getPageNumber(); //获取该页
        final int pageSize=BufferPool.getPageSize();//页大小固定
        byte[] bytes=HeapPage.createEmptyPageData();
        //调用HeapPage类中的方法，返回一个和页大小相同的字节数组，用来向里面写将要读的页的数据
        try{
            FileInputStream in=new FileInputStream(file); //文件输入流读取
            in.skip(pgNo*pageSize); //定位到该pgNo
            int len;
            len=in.read(bytes); //从当前位置开始读PageSize大小数据到bytes内，即读完这一页
            HeapPage findPage=new HeapPage(new HeapPageId(tableid,pgNo),bytes);
            in.close();
            return findPage;

        }
        catch(FileNotFoundException e){
            throw new IllegalArgumentException();
        }
        catch(IOException e){
            throw new IllegalArgumentException();
        }



    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pageId= page.getId();
        int pgNo=pageId.getPageNumber();
        if(pgNo>numPages()){
            throw new IllegalArgumentException("PageNumber out of range");
        }
        final int pageSize=BufferPool.getPageSize();
        RandomAccessFile f=new RandomAccessFile(file,"rw");
        f.seek(pageSize*pgNo);
        byte[] data=page.getPageData();
        f.write(data);
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //文件大小/页的固定大小
        int pageNum=(int)Math.floor((file.length()*1.0)/BufferPool.getPageSize());
        return pageNum;
    }

    // see DbFile.java for javadocs
    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     * 需要加锁、解锁
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    /***
     * 插入新元组的过程为：
     * 1.BufferPool调用HeapFile的insertTuple方法
     * 2.HeapFile调用BufferPool的getPage方法。若该表的页在BufferPool中，直接返回；
     *   若不在，则BufferPool调用HeapFile的readPage方法从磁盘上读取页。
     * 3.找到有空slot的页，插入；并将插入后的页返回给BufferPool
     * 4.BufferPool未满就保存，满了就置换一页
     * 5.此时BufferPool中的页和磁盘上的页是不同的，该页被标记为dirty
     * 6.置换时从BufferPool移除的若为脏页，则调用HeapFile的writePage将其写回磁盘
     *
     * 删除元组同理
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pageList = new ArrayList<>();
        for (int i = 0; i < numPages() + 1; i++) {
            //先从BufferPool中读取page
            HeapPage heapPage;
            if (i < numPages()) {
                heapPage = (HeapPage) Database.getBufferPool().getPage(tid,
                        new HeapPageId(getId(), i), Permissions.READ_WRITE);
            }
            else{ //页数不够就加一页
                BufferedOutputStream bw = new BufferedOutputStream(
                        new FileOutputStream(file, true));
                byte[] emptyData = HeapPage.createEmptyPageData();
                bw.write(emptyData);
                bw.close();
                heapPage = (HeapPage) Database.getBufferPool().getPage(tid,
                        new HeapPageId(getId(), numPages()-1), Permissions.READ_WRITE);
            }
            if (heapPage.getNumEmptySlots() == 0) {
                /**Lab4*/
                Database.getBufferPool().releasePage(tid, heapPage.getId());
                continue; //一直找直到找到有空slot的页
            }
            heapPage.insertTuple(t); //将元组插入
            pageList.add(heapPage);
            return pageList;
        }

        throw new DbException("Tuple can not be added");
    }

    // see DbFile.java for javadocs
    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pageList=new ArrayList<>();
        RecordId recordId = t.getRecordId();
        HeapPageId pid = (HeapPageId)recordId.getPageId();
        if (pid.getTableId() == getId()) {
            int pgNo = pid.getPageNumber();
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,
                    pid, Permissions.READ_WRITE);
            heapPage.deleteTuple(t);
            pageList.add(heapPage);
            return pageList;
        }
        throw new DbException("Tuple can not be deleted");

    }


    protected class HeapFileIterator implements DbFileIterator{
        //需要通过BufferPool的getPage方法来获取页，需要tid和pid
        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> tupleIterator; //可以借助heapPage中的迭代器
        private Integer openPgNo; //打开iterator时需要的页数

        public HeapFileIterator(HeapFile heapFile,TransactionId tid){
            this.heapFile=heapFile;
            this.tid=tid;
            this.tupleIterator=null;
            this.openPgNo=null;

        }

        //定位到heapPage上，调用heapPage的迭代器
        private Iterator<Tuple> getTupleIterator(int pgNo) throws TransactionAbortedException, DbException {
            int tableId= heapFile.getId(); //本类已经生成了tableId,获取它
            HeapPageId pid=new HeapPageId(tableId,pgNo); //生成寻找页时需要的HeapPageId
            HeapPage heapPage=(HeapPage)Database.getBufferPool().
                    getPage(tid,pid,Permissions.READ_ONLY); //通过BufferPool寻找页
            return heapPage.iterator(); //调用给页写好的iterator即可返回元组的iterator
        }

        @Override
        /**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open() throws DbException, TransactionAbortedException {
            openPgNo=0;
            tupleIterator=getTupleIterator(openPgNo);
        }

        @Override
        /** @return true if there are more tuples available,
         * false if no more tuples or iterator isn't open. */
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(openPgNo!=null) {
                while(openPgNo<heapFile.numPages()-1){ //还有页没有打开
                    if(tupleIterator.hasNext()){
                        return true;
                    }
                    else{
                        openPgNo++;
                        tupleIterator = getTupleIterator(openPgNo); //添加迭代
                    }
                }
                return tupleIterator.hasNext();
            }
            else{
                return false;
            }
        }

        @Override
        public Tuple next()throws  DbException,TransactionAbortedException, NoSuchElementException
        {
            if(tupleIterator==null)
                throw new NoSuchElementException("fail to open to file");
            if(!tupleIterator.hasNext())
            {
                while(openPgNo<heapFile.numPages()-1)
                {
                    openPgNo++;
                    tupleIterator=getTupleIterator(openPgNo);
                    if(tupleIterator.hasNext())
                        return tupleIterator.next();
                }
                return null;
            }
            return tupleIterator.next();
        }

        @Override
        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            //重新打开
            close();
            open();
        }

        @Override
        /**
         * Closes the iterator.
         */
        public void close() {
            //重置tupleIterator
            openPgNo=null;
            tupleIterator=null;
        }
    }

    // see DbFile.java for javadocs
    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

}

