#include "AsynchronousBlockInputStream.h"
#include <Common/setThreadName.h>
#include <Common/CurrentThread.h>


namespace DB {

    Block AsynchronousBlockInputStream::readImpl() {
        /// If there were no calculations yet, calculate the first block synchronously
        if (!started) {
            calculate();
            started = true;
        } else    /// If the calculations are already in progress - wait for the result
            pool.wait();

        if (exception)
            std::rethrow_exception(exception);

        Block res = block;
        if (!res)
            return res;

        /// Start the next block calculation
        block.clear();
        next();

        return res;
    }


    void AsynchronousBlockInputStream::next() {
        ready.reset();

        // 向线程池中添加任务, schedule()方法
        pool.schedule([this, thread_group = CurrentThread::getGroup()]() {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};

            try {
                if (first)
                    setThreadName("AsyncBlockInput");

                /// AsynchronousBlockInputStream is used in Client which does not create queries and thread groups
                // AsynchronousBlockInputStream用于不创建查询和线程组的客户端
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);
            }
            catch (...) {
                exception = std::current_exception();
                ready.set();
                return;
            }

            //具体实现
            calculate();
        });
    }


    void AsynchronousBlockInputStream::calculate() {
        try {
            if (first) {
                first = false;
                children.back()->readPrefix();
            }

            //读取到一个block
            block = children.back()->read();
        }
        catch (...) {
            exception = std::current_exception();
        }

        //发出事件的信号, 如果auto_Reset为true, 则只有一个等待事件的线程可以恢复执行; 如果auto_Reset为false, 则所有等待的线程都可以继续执行
        ready.set();
    }

}

