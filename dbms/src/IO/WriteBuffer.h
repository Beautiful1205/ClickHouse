#pragma once

#include <algorithm>
#include <cstring>
#include <memory>
#include <iostream>

#include <Common/Exception.h>
#include <IO/BufferBase.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
}


/** A simple abstract class for buffered data writing (char sequences) somewhere.
  * Unlike std::ostream, it provides access to the internal buffer,
  *  and also allows you to manually manage the position inside the buffer.
  *
  * The successors must implement the nextImpl() method.
  */
class WriteBuffer : public BufferBase
{
public:
    WriteBuffer(Position ptr, size_t size) : BufferBase(ptr, size, 0) {}
    void set(Position ptr, size_t size) { BufferBase::set(ptr, size, 0); }

    /** write the data in the buffer (from the beginning of the buffer to the current position);
      * set the position to the beginning; throw an exception, if something is wrong
      *
      * 将buffer中的数据写到下一层目标中
      * 将pos指针移动到working_buffer的起始位置; 如果有问题, 则抛出异常
      *
      * @张晓 简单讲, next()是清理working_buffer的作用。清理后将 pos 指针移动到 begin_pos
      * 比如 将working_buffer中的数据，从begin_pos 到pos位置的数据，逐步写入到文件
      * 比如 从文件中读取一批数据到buffer中，将pos移动到begin_pos位置，让上层读取
      */
    inline void next()
    {
        if (!offset())
            return;
        bytes += offset();

        try
        {
            nextImpl();//重点看下
        }
        catch (...)
        {
            /** If the nextImpl() call was unsuccessful, move the cursor to the beginning,
              * so that later (for example, when the stack was expanded) there was no second attempt to write data.
              */
            pos = working_buffer.begin();
            throw;
        }

        pos = working_buffer.begin();
    }

    /** it is desirable in the successors to place the next() call in the destructor,
      * so that the last data is written
      */
    virtual ~WriteBuffer() {}


    inline void nextIfAtEnd()
    {
        if (!hasPendingData())//判断buffer是否被写满了(pos == working_buffer.end())，如果写满了 则调 用next()方法，将buffer中的数据写到下一层目标中
            next();
    }

    /// 同ReadBuffer的read方法,
    /// 从from中拷贝n个字节写到buffer中
    void write(const char * from, size_t n)
    {
        size_t bytes_copied = 0;

        while (bytes_copied < n) //判断是否已经拷贝了n个字节
        {
            nextIfAtEnd();
            size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
            memcpy(pos, from + bytes_copied, bytes_to_copy);
            pos += bytes_to_copy;
            bytes_copied += bytes_to_copy;
        }
    }


    inline void write(char x)
    {
        nextIfAtEnd();
        *pos = x;
        ++pos;
    }

private:
    /** Write the data in the buffer (from the beginning of the buffer to the current position).
      * Throw an exception if something is wrong.
      */
    //重点看实现类中的方法
    virtual void nextImpl() { throw Exception("Cannot write after end of buffer.", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER); }
};


using WriteBufferPtr = std::shared_ptr<WriteBuffer>;


}
