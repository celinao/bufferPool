/**
 * Group:
 * Celina Ough 9074747438
 * Atharva Kudkilwar 9081348659
 * Alex Reichart 9079824422 
 * 
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}


void BufMgr::advanceClock() {
  // Advances clockHand by 1
  clockHand += 1; 

  // Checks if clockHand has exceeded number of frames and resets.
  if(clockHand == numBufs){ 
    clockHand = 0;
  }
}

void BufMgr::allocBuf(FrameId& frame) {
  std::uint32_t pinned_num = 0; // Keeps track of no.of pinned frames.

  while (pinned_num < numBufs) {
    // Uses clock policy
    advanceClock();

    // If frame doesn't have a valid page, we can allocate directly.
    if (bufDescTable[clockHand].valid == false) {
      frame = bufDescTable[clockHand].frameNo;
      return;
    }

    // If frame contains a valid page, check for refbit.
    if (bufDescTable[clockHand].refbit == true) { 
      // Set refbit to false 
      bufDescTable[clockHand].refbit = false;
    }else {
      // Increment pinned counter and clock if pinCnt is not 0.
      if (bufDescTable[clockHand].pinCnt > 0) { 
        pinned_num++;
      }
      // Flush before allocating frame if dirty and unpinned.
      else if (bufDescTable[clockHand].dirty == true) {
        flushFile(bufDescTable[clockHand].file);
        frame = bufDescTable[clockHand].frameNo;
        return;
      }// Unpinned and unmodified frame can be allocated directly. 
      else if (bufDescTable[clockHand].dirty == false) {
        frame = bufDescTable[clockHand].frameNo;
        return;
      }
    }
  }
  throw BufferExceededException();
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  FrameId currentFrame = 0; 
  try{
    hashTable.lookup(file, pageNo, currentFrame);
    // Case 2: Page is in buffer pool
    bufDescTable[currentFrame].refbit = true;
    bufDescTable[currentFrame].pinCnt += 1;

    // Return pointer to frame containing page via page parameter. 
    page = &bufPool[currentFrame];

  }catch(const HashNotFoundException &e){
    // Case 1: Page is not in buffer pool 
    // allocate Buffer frame 
    allocBuf(currentFrame); 
    
    // read page from disk to buffer pool frame 
    bufPool[currentFrame] = file.readPage(pageNo); 

    // Set pinCnt to 1
    bufDescTable[currentFrame].Set(file, bufPool[currentFrame].page_number());  

    // Insert page into hashtable 
    hashTable.insert(file, bufPool[currentFrame].page_number(), currentFrame); 

    // Return pointer to frame containing page via page parameter. 
    page = &bufPool[currentFrame];
  }
} 

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
  FrameId currentFrame = 0; 
  try{
    // Check if page is in buffer pool
    hashTable.lookup(file, pageNo, currentFrame);
    if(bufDescTable[currentFrame].pinCnt == 0){
      throw PageNotPinnedException("BufMgr::unPinPage", pageNo, currentFrame);
    }
    if(dirty){
      // set page to dirty
      bufDescTable[currentFrame].dirty = true;
    }
    // unpin one page
    bufDescTable[currentFrame].pinCnt -= 1;
  }catch(const HashNotFoundException &e){}
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
  FrameId currentFrame = 0; 
  try{
    hashTable.lookup(file, pageNo, currentFrame);
    
    // Set is invoked on the frame 
    bufDescTable[currentFrame].Set(file, bufPool[currentFrame].page_number());

    // return pageNumber of new page via pageNo & pointer to buffer frame via page parameter
    page = &bufPool[currentFrame]; 
    *page = bufPool[bufDescTable[currentFrame].frameNo];
    pageNo =  bufPool[bufDescTable[currentFrame].frameNo].page_number();
    return;
  }catch(const HashNotFoundException &e ){}

  // allocBuf() is called to obtain a buffer pool frame. 
  allocBuf(currentFrame);
  
  // Allocate an empty page in the specified file using file.allocatePage() 
  bufPool[currentFrame] = file.allocatePage();

  // Set is invoked on the frame 
  bufDescTable[currentFrame].Set(file, bufPool[currentFrame].page_number());
  
  // entry is inserted into hashTable 
  hashTable.insert(file, bufPool[currentFrame].page_number(), bufDescTable[currentFrame].frameNo);

  // return pageNumber of new page via pageNo & pointer to buffer frame via page parameter
  page = &bufPool[currentFrame]; 
  *page = bufPool[currentFrame];
  pageNo = bufPool[currentFrame].page_number();
}

void BufMgr::flushFile(File& file) {
  
  // Scan bufDecTable for all pages belonging to file.
  for(FrameId currentFrame = 0; currentFrame < numBufs; currentFrame++){

    // Check if file matches
    if(bufDescTable[currentFrame].file == file){ 

      // Check if page is valid 
      if(bufDescTable[currentFrame].valid == false){ 
        throw BadBufferException(currentFrame, bufDescTable[currentFrame].dirty, bufDescTable[currentFrame].valid, bufDescTable[currentFrame].refbit);
      }
      // Check if page is unpin and can be removed
      else if(bufDescTable[currentFrame].pinCnt > 0){ 
        throw PagePinnedException("BufMgr::flushFile", bufDescTable[currentFrame].pageNo, currentFrame);
      }
      // write dirty page and remove
      else if (bufDescTable[currentFrame].dirty){
        bufDescTable[currentFrame].file.writePage(bufPool[currentFrame]);
        bufDescTable[currentFrame].dirty = false;
        hashTable.remove(file, bufDescTable[currentFrame].pageNo);
        bufDescTable[currentFrame].clear();
      }
      // remove clean page
      else{
        hashTable.remove(file, bufDescTable[currentFrame].pageNo);
        bufDescTable[currentFrame].clear();
      }
    }
  }
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
  FrameId currentFrame;
  try{
    // check if page exists in bufferpool
    hashTable.lookup(file, PageNo, currentFrame); 

    // remove page from hashTable, bufDescTable, and file
    hashTable.remove(file, PageNo);
    bufDescTable[currentFrame].clear();
    file.deletePage(PageNo);

  } catch(const HashNotFoundException &e) {  
    // page is not in buffer pool so delete from file
    file.deletePage(PageNo); 
  }  
}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
