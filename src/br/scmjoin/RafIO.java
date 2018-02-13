package br.scmjoin;
import java.io.File;
import java.io.RandomAccessFile;

/**
 * The RafIO provides methods for reading blocks from the container file and writing blocks to the container file.
 * If the container file does not exist, the RafIO creates a new container file.
 * Used blocks inside the container file can be released and new blocks can be allocated for use.
 **/
public class RafIO {
    private RandomAccessFile rafContainer   = null;
    
    private static int maxContainerSize = 10000000; 
    private static int PAGETYPE_USED = 1;
    private static int blockSize = 8192;
    private static int blockHeaderSize = 8;
    private static int fileHeaderSize = 11;
    
    private int totalHeaderSize;
    private int blockBytesUsed;
    
    private int              containerNo     = -1;
    private String           filename        = "";
    
    private byte[] blockRead;    
         
    private int              maxBlockNo      = -1;
    private boolean[]        blockUsed       = null;
    
    /**
     * Creates a new container. 
     * *
     * If the filename contains a directory specification then the existence of the directory has to be checked. 
     * If a file with filename already exists it has to be deleted. 
     * *
     * Header of file: ContainerNo (byte 0) - number of container
     * 				   BlockSize (bytes 1, 2, 3) - size of blocks
     * 				   PAGETYPE_USED (byte 4) - byte that indicate block used (1)
     * 				   MaxBlockNo (bytes 5, 6, 7, 8) - Next block free for write
     * 				   FileHeaderSize (bytes 9, 10) - Size of header's file
     * 				   FileHeader (bytes 11 until 11+FileHeaderSize) 
     * @param containerNo container number
     * @param filename name of the container file
     * @param blockSize block size
     * @param header bytes of file's header
     * @throws Exception
     */    
    
    
    public RafIO (int containerNo, String filename, byte[] header) throws Exception  
    // constructor to create container
    {
      
        this.containerNo = containerNo;
        this.filename    = filename;        
       
        if (this.filename.lastIndexOf ('/') != -1) // filename contains directory specification -> check directory
        {
            String directory = this.filename.substring (0, this.filename.lastIndexOf('/'));

            try // check existence of directory
            {
                File fileDirectory = new File (directory);

                if (!fileDirectory.exists())
                {
                    fileDirectory.mkdir ();
                    System.out.println ("Container directory " + directory + " created.");
                }
            }
            catch (SecurityException e)
            {
                System.out.println ("Creating directory " + directory + " failed.");
            }
        }

		try // delete file if file already exists
		{
			File file = new File (this.filename);

			if (file.exists()) file.delete();
		}
		catch (Exception e)
		{
			System.out.println ("Deleting file " + this.filename + " failed.");
		}

	    try // create container file in asynchron write mode and flush finally (faster for creation)
        {
            this.rafContainer = new RandomAccessFile (this.filename, "rw");
        }
        catch (Exception e)
        {
            System.out.println ("Creating file " + this.filename + " failed.");
        }

        // create container catalog (== index block)
        byte[] containerCatalog = new byte[blockSize];
        for (int i=0; i < blockSize; i++) containerCatalog[i] = 0;
        byte [] tempbyte = RafIOCalc.getByteArray(this.containerNo);
        containerCatalog[0] = tempbyte[0];
        
        tempbyte = RafIOCalc.getByteArray (blockSize);        
        containerCatalog[1] = tempbyte[2]; // set container blockSize to bytes 0 and 1
        containerCatalog[2] = tempbyte[1];
        containerCatalog[3] = tempbyte[0];
        
        tempbyte = RafIOCalc.getByteArray (PAGETYPE_USED);
        containerCatalog[4] = tempbyte[0];
        
        tempbyte = RafIOCalc.getByteArray (1); 
        containerCatalog[5] = tempbyte[0]; // set next free block to 1 (0 is header)
    	containerCatalog[6] = tempbyte[1];
    	containerCatalog[7] = tempbyte[2];
    	containerCatalog[8] = tempbyte[3];
                
        if (header != null) {
        	tempbyte = RafIOCalc.getByteArray(header.length);
        	containerCatalog[9] = tempbyte[0];
        	containerCatalog[10] = tempbyte[1];
        	for (int i=0; i < header.length; i++){
        		containerCatalog[i+fileHeaderSize]= header[i];
        	}
        } else {        	
        	containerCatalog[9] = 0;
        	containerCatalog[10] = 0;
        }        
        
        this.totalHeaderSize = fileHeaderSize + (header!=null?header.length:0);
        
        this.blockUsed = new boolean [maxContainerSize + 1]; // check of last entry -> max container size reached
        for (int i=0; i < maxContainerSize+1; i++) this.blockUsed[i] = false;
        
        try // write container catalog
        {
            this.rafContainer.write (containerCatalog);
            this.blockUsed[0] = true;
            this.maxBlockNo = 1;
        }
        catch (Exception e)
        {
            System.out.println ("Writing container catalog into file " + this.filename + " failed.");
        }

        // flush and close created container file and invoke constructor to open existing container file
        try
        {
        	this.rafContainer.getFD().sync ();
            //this.rafContainer.close ();
        }
        catch (Exception e)
        {
            System.out.println ("Closing created container file " + this.filename + " failed.");
        }

        System.out.println ("Container file " + this.filename + " created.");
    } // RafIO
    
    public RafIO() {
		super();
	}

	/**
     * Opens an already existing container. 
     * *
     * Create a RandomAcessFile rafContainer 
     * Read the first four bytes (blockNo, blockSize) 
     * Read entire file to get maxBlockNo e point to last block
     * *
     * @param containerNo container number
     * @param filename container filename
     * @throws Exception
     */
    public RafIO (int containerNo, String filename) throws Exception
    // constructor to open already existing container
    {
        this.containerNo = containerNo;
        this.filename    = filename;

        if (filename.lastIndexOf ('/') != -1) // containerFilename contains directory specification -> check directory
        {
            String directory = this.filename.substring (0, this.filename.lastIndexOf('/'));

            try // check existence of container directory
            {
                File fileDirectory = new File (directory);

                if (!fileDirectory.exists())
                {
                    System.out.println ("Directory " + directory + " does not exist.");
                }
            }
            catch (SecurityException e)
            {
                System.out.println ("Checking directory " + directory + " failed.");
            }
        }

        //long cnt_lastmodified = 0;	
        //long cnt_length = 0;		
        try // check container file
        {
            File file = new File (this.filename);

            if (!file.exists()) System.out.println ("Container file " + this.filename + " does not exist.");
            //cnt_lastmodified = file.lastModified();	
            //cnt_length = file.length();				
        }
        catch (Exception e)
        {
        	System.out.println ( "Checking existence of file " + this.filename + " failed.");
        }
		
		try // open existing container file
        {
            this.rafContainer = new RandomAccessFile (this.filename, "rws"); // container file is later explicity forced
        }
        catch (Exception e)
        {
        	System.out.println ("Opening container file " + this.filename + " failed.");
        }

        // read block 
        byte[] bytelist = new byte[fileHeaderSize];
        try
        {
            this.rafContainer.read (bytelist);
        }
        catch (Exception e)
        {
        	System.out.println ("Reading index data from container file " + this.filename + " failed.");
        }
        byte[] tempbyte= {bytelist[1], bytelist[2],bytelist[3],0};
        blockSize  = RafIOCalc.getInt(tempbyte); // get block size  from bytes 1,2 and 3
        
        tempbyte[0] = bytelist[4];
        tempbyte[1] = 0;
        tempbyte[2] = 0;
        tempbyte[3] = 0;
        PAGETYPE_USED = RafIOCalc.getInt(tempbyte); // get pagetype_used  from byte 4
        
        tempbyte[0] = bytelist[5];
        tempbyte[1] = bytelist[6];
        tempbyte[2] = bytelist[7];
        tempbyte[3] = bytelist[8];
        this.maxBlockNo = RafIOCalc.getInt(tempbyte); // get next free block from bytes 5,6, 7 and 8
        
        tempbyte[0] = bytelist[9];
        tempbyte[1] = bytelist[10];
        tempbyte[2] = 0;
        tempbyte[3] = 0;
        this.totalHeaderSize = RafIOCalc.getInt(tempbyte) + fileHeaderSize; // get block header size  from bytes 9 and 10
        
        
        this.blockUsed = new boolean [maxContainerSize + 1]; // check of last entry -> max container size reached
        for (int i=0; i < maxContainerSize+1; i++) {
        	if (i < this.maxBlockNo)
        		this.blockUsed[i] = true;
        	else
        		this.blockUsed[i] = false;
        }
        
        //int blockNo = 0;
        this.blockUsed[0] = true;
        //blockNo++;
        
      	/* maxblockno is written in header of file
        // scan entire container file to get numberOfBlocks, block usage, and maxLSN information
       	this.maxBlockNo = 0;      	

        try
        {
        	this.rafContainer.seek (blockSize); // move file pointer to first block after container catalog 
        	

        	bytelist = new byte[fileHeaderSize];

        	while (true) // scan entire file
        	{
        		this.rafContainer.read (bytelist); // read 7B pageNo, pageType
        		if (bytelist[4] == PAGETYPE_USED) this.blockUsed[blockNo] = true; // bytelist[4] is pageType
        		else this.blockUsed[blockNo] = false;

        		// if a complete block is following -> seek file pointer to next block
        		if (this.rafContainer.getFilePointer() - bytelist.length + blockSize < this.rafContainer.length()) 
        		{
        			this.rafContainer.seek (this.rafContainer.getFilePointer() - bytelist.length + blockSize);
        		}
        		else break; // all blocks of container file are scanned
        		blockNo++;
        	} // while
        	this.maxBlockNo = blockNo;
        }
        catch (Exception e)
        {
        	System.out.println ("Scanning container file " + this.filename + " failed. Reason: " + e.getMessage());
        }*/

    } // RafIO
       
    /**
     * Returns the container number.
     * @return containerNo
     */
    public int getContainerNo ()
    {
        return this.containerNo;
    }

    /**
     * Returns the container's filename.
     * @return container filename
     */
    public String getContainerFilename ()
    {
        return this.filename;
    }
    
    /**
     * Returns the block size.
     * @return block size
     */
    public static int getBlockSize ()
    {
        return blockSize;
    } // getBlockSize

    /**
     * Returns the block header size.
     * @return block header size
     */
    public static int getBlockHeaderSize ()
    {
        return blockHeaderSize;
    } // getBlockHeaderSize
    
    /**
     * Returns the bytes written in the last block read.
     * @return block bytes used
     */
    public int getBlockBytesUsed ()
    {
        return this.blockBytesUsed;
    } // getBlockBytesUsed
    
    /**
     * Returns the block header size.
     * @return block header size
     */
    public static int getFileHeaderSize ()
    {
        return fileHeaderSize;
    } // getFileHeaderSize
    
    /**
     * Returns the total header size.
     * @return total header size
     */
    public int getTotalHeaderSize ()
    {
        return this.totalHeaderSize;
    } // getTotalHeaderSize
    
    /**
     * Returns the largest block number.
     * @return maxBlockNo
     */
    public int getMaxBlockNo ()
    {
        return this.maxBlockNo;
    } // getMaxBlockNo

    /**
     * Returns the number of used blocks.
     * @return number of used blocks
     */
    public int getNumberOfUsedBlocks ()
    {
        int numberOfUsedBlocks = 0;

        for (int i=0; i < maxContainerSize; i++) if (this.blockUsed[i] == true) numberOfUsedBlocks++;

        return numberOfUsedBlocks;
    } // getNumberOfUsedBlocks
    
    /**
     * Returns true, if this block (part of pageNo) is marked as used.
     * @return true, if this block (part of pageNo) is marked as used.
     */
    public boolean getBlockIsUsed(int blockNo)
    {
    	synchronized(this.blockUsed) {
    		return this.blockUsed[blockNo];
    	}
    }

    /**
     * Returns filename.
     * @return String filename.
     */
    public String getFileName()
    {
  		return this.filename;
    }

    /**
     * Allocates a new block in the container file.
     * *
     * Allocate the block for the next free block (maxBlockNo)
     * *
     * * *
     * Header of block: ContainerNo (byte 0) - number of container
     * 				    BlockNo (bytes 1, 2, 3) - number of block
     * 				    BlockType (byte 4) - byte that indicate block used (1)
     * 				    BytesUsed (bytes 5, 6, 7) - number of bytes written within block
     * @throws Exception
     */
   	public  byte[] allocateBlock () throws Exception
    {
   		int blockNo = this.maxBlockNo;
    	    	
        blockNo = 1; // block 0 is used as container catalog, block1 is used for before image
        while (this.blockUsed[blockNo]) blockNo++;
        

        if (blockNo == maxContainerSize) System.out.println ("Max container size for container file " + this.filename + " reached.");

        this.blockUsed[blockNo] = true;

        // clear bytes
        byte[] block = new byte[blockSize];
        for (int i=0; i < blockSize; i++) block[i] = 0;

        // set pageNo and pageType of block
        byte[] tempbyte;
        tempbyte = RafIOCalc.getByteArray(this.containerNo);
        block[0] = RafIOCalc.getByte(tempbyte, 0);    // set containerNo   >
        
        tempbyte = RafIOCalc.getByteArray(blockNo);
        block[1] = RafIOCalc.getByte(tempbyte, 2);             // set blockNo        > = pageNo 
        block[2] = RafIOCalc.getByte(tempbyte, 1);             // set blockNo        >
        block[3] = RafIOCalc.getByte(tempbyte, 0);             // set blockNo       >
        
        tempbyte = RafIOCalc.getByteArray(PAGETYPE_USED);
        block[4] = RafIOCalc.getByte(tempbyte, 0);           // set blockType
        
        tempbyte = RafIOCalc.getByteArray(0);
        block[5] = RafIOCalc.getByte(tempbyte, 2);             // set blocksUsed initially zero 
        block[6] = RafIOCalc.getByte(tempbyte, 1);             
        block[7] = RafIOCalc.getByte(tempbyte, 0);                     

        //System.out.println ("BlockNo " + blockNo + " allocated in container file " + this.filename + ".");
        
        return block;
    } // allocateBlock
   	
   	/**
   	 * Releases a block. 
   	 * 
   	 * It clears the page in directly and uses to write an empty block into the container file to exploit container logging. 
   	 * 
   	 * It creates a cleared block and keeps the <code>pageNo</code> and <code>blockNo</code> available. If the <code>pageType</code> 
   	 * is cleared this indicates that the block is no longer in use. After that an empty block is written to the container file. 
   	 * Now the the corresponding entry in the <code>blockUsed[]</code> byte-array is set to false because only used blocks are written to disk. 
   	 *  
   	 * @param blockNo block number
   	 * @throws Excetion
   	 */
    public  void releaseBlock (int blockNo) throws Exception
    {
        if (!this.blockUsed[blockNo]) System.out.println ("BlockNo " + blockNo + " not used.");
        
        // create cleared block keep pageNo/blockNo available, pageType is cleared -> block not in use!
        byte[] block = new byte[blockSize];
        byte[] tempbyte;
        tempbyte = RafIOCalc.getByteArray (this.containerNo);
        block[0] = RafIOCalc.getByte(tempbyte, 0);
        
        tempbyte = RafIOCalc.getByteArray (blockNo);
        block[1] = RafIOCalc.getByte(tempbyte, 2);
        block[2] = RafIOCalc.getByte(tempbyte, 1);
        block[3] = RafIOCalc.getByte(tempbyte, 0);
        
        tempbyte = RafIOCalc.getByteArray(0);
        block[4] = RafIOCalc.getByte(tempbyte, 0);           // set blockType
        
        tempbyte = RafIOCalc.getByteArray(0);
        block[5] = RafIOCalc.getByte(tempbyte, 2);             // set blocksUsed initially zero 
        block[6] = RafIOCalc.getByte(tempbyte, 1);             
        block[7] = RafIOCalc.getByte(tempbyte, 0);  
        
        for (int i=4; i < blockSize; i++) block[i] = 0;

        // write empty block to container file
        writeBlock (0, block);

        // set block unused after write operation, because only used blocks are written
        this.blockUsed[blockNo] = false;

        //System.out.println ("BlockNo " + blockNo + " released in container file " + this.filename + ".");
    } // releaseBlock

    /**
     * Reads the block with block number <code>blockNo</code>.
     * *
     * If (blockNo > maxBlockNo) then Exception is thrown. 
     * If the caller tries to read an unused block the method returns null.
     * If everything is ok then the method retrieves the block from the container file and returns it to the caller. 
     * 
     * @param blockNo block number
     * @throws Exception
     */
     public  byte[] readBlock (int blockNo) throws Exception
     {
    	 long seekAddress=0;
         if ((blockNo == this.maxBlockNo)) // < 2 -> container catalog and before image not accessible
         {
             System.out.println ("Invalid blockNo " + blockNo + " in container " + this.filename + ".");
             return null;
         }

         if (this.blockUsed[blockNo] == false)
         {
             return null;
         }

         try
         {
//         	 byte[] blockRead = new byte[blockSize];
             if (blockRead == null)
        		 blockRead = new byte[blockSize];
        	   
         	
             // load block
         	 seekAddress = (long)blockNo * (long)blockSize;
             rafContainer.seek (seekAddress);
             rafContainer.read (blockRead);
             
             if (blockRead[4] == 0) // check if block is really used (has a valid type)
             {
             	this.blockUsed[blockNo] = false;
             	return null;
             }
             
             byte[] tempbyte =  new byte[4];
             tempbyte[0] = blockRead[5];             // set blocksUsed initially zero 
             tempbyte[1] = blockRead[6];             
             tempbyte[2] = blockRead[7];
             this.blockBytesUsed = RafIOCalc.getInt(tempbyte);
             
         	 //System.out.println ("Read blockNo " + blockNo + " from container file " + this.filename + ".");
             
             return blockRead;
         }
         catch (Exception e)
         {
        	 System.out.println ("Reading blockNo " + blockNo + " from container file " + this.filename + " failed.");
             return null; // pseudo-return -> addException throws exception
         }
     } // readBlock

     
     /**
      * Writes a block to disk.
      * *
      * The block is written on the next free block  
      * * 
      * @param block the block that shall be written
      * @return the number of block that was written
      * @throws Exception 
      */
     public  int writeBlock (int bytesUsed, byte[] block) throws Exception
     {
    	 int blockNo = this.maxBlockNo;
    	 byte[] blockWithHeader, blockHeader;
    	 long seekAddress=0;
         try // write block into container file
         {
        	 blockWithHeader = this.allocateBlock();
        	 for (int i=0; i<blockSize-blockHeaderSize; i++)
        		 blockWithHeader[i+blockHeaderSize]=block[i];
        	 
        	 byte [] tempbyte = RafIOCalc.getByteArray(bytesUsed);
        	 blockWithHeader[5] = tempbyte[0];             // set blocksUsed  
        	 blockWithHeader[6] = tempbyte[1];             
        	 blockWithHeader[7] = tempbyte[2];  
        	        	        	 
        	 seekAddress = (long)blockNo * (long)blockSize;
             rafContainer.seek  (seekAddress);  // seek container file pointer to blockNo
             rafContainer.write (blockWithHeader);                     // write page content from buffer into container file
             this.maxBlockNo++;
             this.blockUsed[blockNo] = true;
             //update new maxBlockNo on header file (block 0)
             blockHeader = readBlock(0);
             tempbyte = RafIOCalc.getByteArray (this.maxBlockNo); 
             blockHeader[5] = tempbyte[0]; // set next free block 
             blockHeader[6] = tempbyte[1];
             blockHeader[7] = tempbyte[2];
             blockHeader[8] = tempbyte[3];                                 
             
             writeBlock(0, blockHeader, 0);

             //System.out.println ("BlockNo " + blockNo + " written into container " + this.filename + ".");
         }
         catch (Exception e)
         {
        	 System.out.println ("Writing blockNo " + blockNo + " into container " + this.filename + " failed.");
        	 System.out.println (e.getMessage());
        	 e.printStackTrace();
         }
    	 
         return blockNo; 
            
     } // writeBlock
     
     /**
      * Writes a block to disk.
      * *
      * The block is written on the specified block  
      * * 
      * @param block the block that shall be written
      * @param blockNo number of block 
      * @throws Exception 
      */
     public  void writeBlock (int bytesUsed, byte[] block, int blockNo) throws Exception
     {
    	 long seekAddress=0;
    	 if (blockNo != 0 && block.length > blockSize)
    		 throw new Exception("Block is greater than blocksize");
    	 
         try // write block into container file
         {
        	        	 
        	 if (blockNo != 0) {
	        	 byte [] tempbyte = RafIOCalc.getByteArray(bytesUsed);
	             block[5] = tempbyte[0];             // set blocksUsed  
	             block[6] = tempbyte[1];             
	             block[7] = tempbyte[2];  
        	 }
        	 seekAddress = (long)blockNo * (long)blockSize;
             rafContainer.seek  (seekAddress);  // seek container file pointer to blockNo
             rafContainer.write (block);                     // write page content from buffer into container file
             this.blockUsed[blockNo] = true;

             //System.out.println ("BlockNo " + blockNo + " written into container " + this.filename + ".");
         }
         catch (Exception e)
         {
        	 System.out.println ("Writing blockNo " + blockNo + " into container " + this.filename + " failed.");
        	 System.out.println (e.getMessage());
         }
            
     } // writeBlock
     
     
     
     /**
      * Close the container.
      * *
      * @throws Exception 
      */
     public void close () throws Exception
     {
    	 this.rafContainer.close ();
     } // close   

	// MAIN  
	public static void main( String args[] ) { 
    	 RafIO raf;
    	 byte[] header = new byte[1];
    	 header[0]= 1;
    	 try {
      		 raf = new RafIO (1, "C:/RAFIO/ARQUIVO.T", header);
    		 int numero = 80;
    		 
    		 byte[] block = raf.allocateBlock();
    		 
    		 byte[] tempbyte = RafIOCalc.getByteArray(numero);
    		 int pos = 0;

    		 for (int i = 0; i< tempbyte.length; i++) {
    			 block[pos] = tempbyte[i];
    			 pos++;
    		 }	 
    			 
    		 raf.writeBlock(pos, block);
    		 byte[] btl = raf.readBlock(1);
    		 pos = 0;
    		 for (int i = 5; i<2; i++) {
    			 tempbyte[pos] = btl[i];
    			 pos++;
    		 }
    		
    		 System.out.println(RafIOCalc.getInt(btl));  		 
    		 
    		
    	 } 
    	 catch (Exception e) {System.out.println (e.getMessage());
    	 }	 
     }

    
}