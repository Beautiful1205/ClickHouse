#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>


namespace DB {

    Block MergeTreeBlockOutputStream::getHeader() const {
        return storage.getSampleBlock();
    }


    void MergeTreeBlockOutputStream::write(const Block &block) {
        storage.delayInsertOrThrowIfNeeded();

        /// split Block Into Parts 将block分割成多个part (一个partition中有多个part)
        auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block);
        for (auto &current_block : part_blocks) {
            Stopwatch watch;

            //写temp part
            MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block);
            //重命名temp part
            storage.renameTempPartAndAdd(part, &storage.increment);
            //添加新part
            PartLog::addNewPart(storage.global_context, part, watch.elapsed());

            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            ///启动异步合并. 如果是应该进行merge且background_pool中有空余线程, 则会执行merge
            storage.background_task_handle->wake();//唤醒background_pool中的merge线程
        }
    }

}
