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
        frame = bufDescTable[clockHand].frameNo;
        return;
      }
    }
  }

  throw BufferExceededException();
} // Atharva


void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
  // Reads page??? idk how in depth it wants us to read it. 

  uint32_t startingClockHand = clockHand;
  advanceClock();

  // Case 2: Frame is already in buffer pool
  // Not sure if this works while one is FrameId and one is uint32_t
  while(startingClockHand != clockHand){ 
    try{
      hashTable.lookup(file, pageNo, clockHand);

      // Sets refbit to true & increments pinCnt
      bufDescTable[clockHand].refbit = true;
      bufDescTable[clockHand].pinCnt += 1;
      
      // Sets page to equal a pointer to the frame containing the page
      // IDK if this is correct. 
      Page newPage = file.allocatePage();
      page = &newPage;
      return; 
    }catch(const HashNotFoundException &e){
      advanceClock();
    }
  }

  // Case 1: Page is not in buffer pool. 
  allocBuf(clockHand); // allocate a buffer frame. 
  file.readPage(pageNo); // read page from disk into buffer pool frame. 
  hashTable.insert(file, pageNo, clockHand); // Insert page into hashtable. 
  bufDescTable[clockHand].Set(file, pageNo); // Set up page with refBit and pinCnt
  // return a pointer to the frame containing the page via the page parameter. 
  Page newPage = file.allocatePage();
  page = &newPage;
} 

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {} // Alex

void BufMgr::flushFile(File& file) {}

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
