#pragma once

#include <Core/Defines.h>
#include <algorithm>


namespace DB {


/** Base class for ReadBuffer and WriteBuffer.
  * Contains common types, variables, and functions.
  *
  * Read/WriteBuffers 和 istream/ostream 的作用类似, 但是效率更高.
  * ReadBuffer and WriteBuffer are similar to istream and ostream, respectively.
  * They have to be used, because using iostreams it is impossible to effectively implement some operations.
  * For example, using istream, you can not quickly read string values from a tab-separated file,
  *  so that after reading, the position remains immediately after the read value.
  * (The only option is to call the std::istream::get() function on each byte, but this slows down due to several virtual calls.) 虚方法调用比较耗时
  *
  * Read/WriteBuffers可以直接高效的操作internal buffer. 只有一个虚方法nextImpl(), 用到的也比较少.
  * Read/WriteBuffers provide direct access to the internal buffer, so the necessary operations are implemented more efficiently.
  * Only one virtual function nextImpl() is used, which is rarely called:
  * - in the case of ReadBuffer - fill in the buffer with new data from the source; 从source读取数据填充到ReadBuffer
  * - in the case of WriteBuffer - write data from the buffer into the receiver.    在WriteBuffer写数据发送到receiver
  *
  * Read/WriteBuffer可以独占或不独占一份内存. 不独占的时候, 可以不用复制, 直接读取一块已经存在的内存.
  * Read/WriteBuffer can own or not own an own piece of memory.
  * In the second case, you can effectively read from an already existing piece of memory / std::string without copying it.
  */
    class BufferBase {
    public:
        /** Cursor in the buffer. The position of write or read. */
        using Position = char *;//buffer中的游标位置, 表示读/写的位置; Position变量保存的地址所在内存单元中的数据类型为char类型

        /** A reference to the range of memory. */
        struct Buffer {
            Buffer(Position begin_pos_, Position end_pos_) : begin_pos(begin_pos_), end_pos(end_pos_) {}

            inline Position begin() const { return begin_pos; }

            inline Position end() const { return end_pos; }

            inline size_t size() const { return size_t(end_pos - begin_pos); }

            inline void resize(size_t size) { end_pos = begin_pos + size; }

            inline void swap(Buffer &other) {
                std::swap(begin_pos, other.begin_pos);
                std::swap(end_pos, other.end_pos);
            }

        private:
            Position begin_pos;
            Position end_pos;        /// 1 byte after the end of the buffer
        };

        /** The constructor takes a range of memory to use for the buffer.
          * offset - the starting point of the cursor.
          * ReadBuffer must set it to the end of the range, and WriteBuffer - to the beginning.
          */
        BufferBase(Position ptr, size_t size, size_t offset)
                : pos(ptr + offset), working_buffer(ptr, ptr + size), internal_buffer(ptr, ptr + size) {}

        void set(Position ptr, size_t size, size_t offset) {
            internal_buffer = Buffer(ptr, ptr + size);
            working_buffer = Buffer(ptr, ptr + size);
            pos = ptr + offset;
        }

        /// get buffer
        inline Buffer &internalBuffer() { return internal_buffer; }

        /// get the part of the buffer from which you can read / write data
        inline Buffer &buffer() { return working_buffer; }

        /// get (for reading and modifying) the position in the buffer
        inline Position &position() { return pos; }

        /// offset in bytes of the cursor from the beginning of the buffer
        /// 指针距离缓冲区起始位置的字节偏移量
        inline size_t offset() const { return size_t(pos - working_buffer.begin()); }

        /// How many bytes are available for read/write
        inline size_t available() const { return size_t(working_buffer.end() - pos); }

        inline void swap(BufferBase &other) {
            internal_buffer.swap(other.internal_buffer);
            working_buffer.swap(other.working_buffer);
            std::swap(pos, other.pos);
        }

        /** How many bytes have been read/written, counting those that are still in the buffer. */
        size_t count() const {
            return bytes + offset();
        }

        /** Check that there is more bytes in buffer after cursor. */
        bool ALWAYS_INLINE hasPendingData() const {
            return pos != working_buffer.end();
        }

        bool isPadded() const {
            return padded;
        }

    protected:
        /// Read/write position.
        Position pos;

        /** How many bytes have been read/written, not counting those that are now in the buffer.
          * (counting those that were already used and "removed" from the buffer)
          * 已经读/写了的buffer中的字节大小（读/写过的数据将从buffer中remove）
          */
        size_t bytes = 0;

        /** A piece of memory that you can use.
          * For example, if internal_buffer is 1MB, and from a file for reading it was loaded into the buffer only 10 bytes,
          *  then working_buffer will be 10 bytes in size (working_buffer.end() will point to the position immediately after the 10 bytes that can be read).
          */
        /**
         * working_buffer是一段可以使用的内存
         * 例如, 如果internal_buffer(内部缓冲区)设置的是1MB, 但是读取一个文件并将数据加载到缓冲区中后, 数据只占了10个字节,
         * 那么working_buffer(工作缓冲区)的大小将是10个字节
         * 注意: working_buffer.end()指针指向的是可读取的10个字节之后的位置
         */
        Buffer working_buffer;  //工作缓冲区(是内部缓冲区的一部分/一个子集)

        /// A reference to a piece of memory for the buffer.
        ///这个是内部buffer，以读/写磁盘文件为例，其指向默认大小1 MB 的一块内存
        Buffer internal_buffer;  //内部缓冲区

        /// Indicator of 15 bytes pad_right
        bool padded{false};
    };


}
