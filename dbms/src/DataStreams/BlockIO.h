#pragma once

#include <DataStreams/IBlockStream_fwd.h>

#include <functional>


namespace DB {

    class ProcessListEntry;

    struct BlockIO {
        BlockIO() = default;

        BlockIO(const BlockIO &) = default;

        ~BlockIO() = default;

        /** process_list_entry should be destroyed after in and after out,
          *  since in and out contain pointer to objects inside process_list_entry (query-level MemoryTracker for example),
          *  which could be used before destroying of in and out.
          */
        std::shared_ptr<ProcessListEntry> process_list_entry;

        BlockOutputStreamPtr out;//负责数据写出, 实现IBlockOutputStream接口, 通过write方法把数据写到磁盘或客户端
        BlockInputStreamPtr in;  //负责数据读入, 实现IBlockInputStream接口, 通过read方法从磁盘或客户端读取数据保存在Block中.

        /// Callbacks for query logging could be set here.
        std::function<void(IBlockInputStream * , IBlockOutputStream * )> finish_callback;
        std::function<void()> exception_callback;

        /// Call these functions if you want to log the request.
        void onFinish() {
            if (finish_callback)
                finish_callback(in.get(), out.get());
        }

        void onException() {
            if (exception_callback)
                exception_callback();
        }

        BlockIO &operator=(const BlockIO &rhs) {
            if (this == &rhs)
                return *this;

            out.reset();
            in.reset();
            process_list_entry.reset();

            process_list_entry = rhs.process_list_entry;
            in = rhs.in;
            out = rhs.out;

            finish_callback = rhs.finish_callback;
            exception_callback = rhs.exception_callback;

            return *this;
        }
    };

}
