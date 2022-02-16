/**
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

using std::cout; // the standard output
using std::endl; // the end of line character, plus flushing the output

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
} // Celina

void BufMgr::allocBuf(FrameId& frame) {
  std::uint32_t pinned_num = 0; // Keeps track of no.of pinned frames.

  while (pinned_num < numBufs) {
    advanceClock();
    // Uses clock policy

    // If frame doesn't have a valid page, we can allocate directly.
    if (bufDescTable[clockHand].valid == false) {
      frame = bufDescTable[clockHand].frameNo;
      return;
    }

    // Valid Set? Yes

    // If frame contains a valid page, check for refbit.
    if (bufDescTable[clockHand].refbit == true) { // Refbit? YES -> CLEAR REFBIT 
      bufDescTable[clockHand].refbit = false;
      // advanceClock();
    }else {
      // Increment pinned counter and clock if pinCnt is not 0.
      if (bufDescTable[clockHand].pinCnt > 0) { // PAGE PINNED? YES
        pinned_num++;
        // advanceClock();
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
    // hashTable.insert(file, bufPool[currentFrame].page_number(), bufDescTable[currentFrame].frameNo); 
    hashTable.insert(file, bufPool[currentFrame].page_number(), currentFrame); 

    // Return pointer to frame containing page via page parameter. 
    page = &bufPool[currentFrame];
  }
} 

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
  FrameId currentFrame = 0; 
  try{
    hashTable.lookup(file, pageNo, currentFrame);
    if(bufDescTable[currentFrame].pinCnt == 0){
      throw PageNotPinnedException("BufMgr::unPinPage", pageNo, currentFrame);
    }
    if(dirty){
      bufDescTable[currentFrame].dirty = true;
    }
    bufDescTable[currentFrame].pinCnt -= 1;
  }catch(const HashNotFoundException &e){}
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
  FrameId frame = 0; 
  try{
    hashTable.lookup(file, pageNo, frame);
    
    // Set is invoked on the frame 
    bufDescTable[frame].Set(file, bufPool[frame].page_number());

    page = &bufPool[frame]; 
    *page = bufPool[bufDescTable[frame].frameNo];
    pageNo =  bufPool[bufDescTable[frame].frameNo].page_number();
    return;
  }catch(const HashNotFoundException &e ){}

  // allocBuf() is called to obtain a buffer pool frame. 
  allocBuf(frame);
  
  // Allocate an empty page in the specified file using file.allocatePage() 
  bufPool[frame] = file.allocatePage();

  // Set is invoked on the frame 
  bufDescTable[frame].Set(file, bufPool[frame].page_number());
  
  // entry is inserted into hashTable 
  hashTable.insert(file, bufPool[frame].page_number(), bufDescTable[frame].frameNo);

  // return pageNumber of new page via pageNo & pointer to buffer frame via page parameter
  page = &bufPool[frame]; 
  *page = bufPool[frame];
  pageNo = bufPool[frame].page_number();

}

void BufMgr::flushFile(File& file) {
  
  // Scan bufDecTable for all pages belonging to file.
  for(FrameId currentFrame = 0; currentFrame < numBufs; currentFrame++){
    if(bufDescTable[currentFrame].file == file){ // Check if file matches

      if(bufDescTable[currentFrame].valid == false){ // Invalid Page
        throw BadBufferException(currentFrame, bufDescTable[currentFrame].dirty, bufDescTable[currentFrame].valid, bufDescTable[currentFrame].refbit);
      }
      else if(bufDescTable[currentFrame].pinCnt > 0){ // Page is currently Pinned
        throw PagePinnedException("BufMgr::flushFile", bufDescTable[currentFrame].pageNo, currentFrame);
      }
      else if (bufDescTable[currentFrame].dirty){

        // write dirty page and remove
        bufDescTable[currentFrame].file.writePage(bufPool[currentFrame]);
        bufDescTable[currentFrame].dirty = false;
        hashTable.remove(file, bufDescTable[currentFrame].pageNo);
        bufDescTable[currentFrame].clear();
      }else{
        // remove clean page
        hashTable.remove(file, bufDescTable[currentFrame].pageNo);
        bufDescTable[currentFrame].clear();
      }
    }
  }
}

void BufMgr::disposePage(File& file, const PageId PageNo) {
    try{
        hashTable.lookup(&file, pageNo, &clockHand);
    } catch(HashNotFoundException e){
        return;
    }
    file.deletePage(PageNo);
    hashTable.remove(&file, pageNo);
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
