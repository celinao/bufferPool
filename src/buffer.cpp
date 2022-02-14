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
    //advanceClock();
    // Uses clock policy
    // If frame doesn't have a valid page, we can allocate directly.
    if (bufDescTable[clockHand].valid == false) {
      frame = bufDescTable[clockHand].frameNo;
      return;
    }
    // If frame contains a valid page, check for refbit.
    if (bufDescTable[clockHand].refbit == true) {
      bufDescTable[clockHand].refbit = false;
      advanceClock();
    }
    else {
      // Increment pinned counter and clock if pinCnt is not 0.
      if (bufDescTable[clockHand].pinCnt > 0) {
        pinned_num++;
        advanceClock();
      }
      // Flush before allocating frame if dirty and unpinned.
      else if (bufDescTable[clockHand].dirty == true) {
        flushFile(bufDescTable[clockHand].file);
        frame = bufDescTable[clockHand].frameNo;
        return;
      }
      // Unpinned and unmodified frame can be allocated directly.
      if (bufDescTable[clockHand].pinCnt == 0 && bufDescTable[clockHand].dirty == false) {
        // Need to remove the existing page before allocating new.
        hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
        frame = bufDescTable[clockHand].frameNo;
        return;
      }
    }
  }

  throw BufferExceededException();
}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  try{
    hashTable.lookup(file, pageNo, clockHand);
    // Case 2: Page is in buffer pool
    bufDescTable[clockHand].refbit = true;
    bufDescTable[clockHand].pinCnt += 1;

    // Return pointer to frame containing page via page parameter. 
    page = &bufPool[clockHand];

  }catch(const HashNotFoundException &e){
    // Case 1: Page is not in buffer pool 
    allocBuf(clockHand); // allocate Buffer frame 
    bufPool[clockHand] = file.readPage(pageNo);// read page from disk to buffer pool frame 
    hashTable.insert(file, pageNo, clockHand); // Insert page into hashtable 
    bufDescTable[clockHand].Set(file, pageNo); // Set pinCnt to 1 

    // Return pointer to frame containing page via page parameter. 
    page = &bufPool[clockHand];
  }
} 

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
  try{
    hashTable.lookup(file, pageNo, clockHand);
    if(bufDescTable[clockHand].pinCnt == 0){
      throw PageNotPinnedException("BufMgr::unPinPage", pageNo, clockHand);
    }
    if(dirty){
      bufDescTable[clockHand].dirty = true;
    }
  }catch(const HashNotFoundException &e){

  }
}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
  try{
    hashTable.lookup(file, pageNo, clockHand);
    std::cout << "HASH ALREADY PRESENT" << endl;
    bufDescTable[clockHand].Print();
    page = &bufPool[clockHand]; 
    *page = bufPool[bufDescTable[clockHand].frameNo];
    pageNo =  bufPool[bufDescTable[clockHand].frameNo].page_number();
    return;
  }catch(const HashNotFoundException &e ){
    
  }

  // Allocate an empty page in the specified file using file.allocatePage() 
  bufPool[clockHand] = file.allocatePage();

  // allocBuf() is called to obtain a buffer pool frame. 
  allocBuf(clockHand);

  // entry is inserted into hashTable 
  hashTable.insert(file, pageNo, clockHand);

  // Set is invoked on the frame 
  bufDescTable[clockHand].Set(file, bufPool[clockHand].page_number());

  // return pageNumber of new page via pageNo & pointer to buffer frame via page parameter
  bufDescTable[clockHand].Print();
  page = &bufPool[clockHand]; 
  *page = bufPool[clockHand];
  pageNo = bufPool[clockHand].page_number();
}

void BufMgr::flushFile(File& file) {
  for(FrameId currentFrame = 0; currentFrame < bufDescTable.size(); currentFrame++){
    // Find all file pages in buffer pool
    if(bufDescTable[currentFrame].file == file){
      // Check if page is part of file 
      if(bufDescTable[currentFrame].valid == false){
        // Invalid Page
        // throw BadBufferException());
      }else if(bufDescTable[currentFrame].pinCnt > 0){
        // Page is currently Pinned
        // throw PagePinnedException("BufMgr::flushFile", pageNum);
      }else if (bufDescTable[currentFrame].dirty){
        FrameId frameNum = bufDescTable[currentFrame].frameNo;
        // write dirty page and remove
        bufDescTable[currentFrame].file.writePage(bufPool[frameNum]);
        bufDescTable[currentFrame].dirty = false;
        hashTable.remove(file, bufDescTable[currentFrame].pageNo);
      }else{
        // remove clean page
        hashTable.remove(file, bufDescTable[currentFrame].pageNo);
      }
    }
  }
  
  // Clear file from bufferpool
  bufDescTable.clear();
}

void BufMgr::disposePage(File& file, const PageId PageNo) {}

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
