#pragma once

#include <Poco/Event.h>

#include <DataStreams/IBlockInputStream.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>


namespace CurrentMetrics {
    extern const Metric QueryThread;
}

namespace DB {

/** Executes another BlockInputStream in a separate thread.
  * This serves two purposes:
  * 1. Allows you to make the different stages of the query execution pipeline work in parallel.
  * 2. Allows you not to wait until the data is ready, and periodically check their readiness without blocking.
  *    This is necessary, for example, so that during the waiting period you can check if a packet
  *     has come over the network with a request to interrupt the execution of the query.
  *    It also allows you to execute multiple queries at the same time.
  */

/** 创建一个异步的BlockInputStream.
  *
  * 在另外一个单独的线程中执行BlockInputStream, 有两个目的:
  * 1-允许查询执行管道的不同阶段能够并行工作;
  * 2-可以不用一直等待, 直到数据准备就绪. 可以定期检查数据的准备状态而不必一直阻塞.
  *
  * 另起一个线程来执行BlockInputStream, 这样一来, 服务在等待周期内还可以检查是否有其他数据包被发送过来取消当前查询的执行.
  * 这样做也便于服务端同时执行多个查询.
  */
    class AsynchronousBlockInputStream : public IBlockInputStream {
    public:
        AsynchronousBlockInputStream(const BlockInputStreamPtr &in) {
            children.push_back(in);
        }

        String getName() const override { return "Asynchronous"; }

        void readPrefix() override {
            /// Do not call `readPrefix` on the child, so that the corresponding actions are performed in a separate thread.
            //不要对子类流调用readPrefix()方法
            if (!started) {
                next();
                started = true;
            }
        }

        void readSuffix() override {
            if (started) {
                pool.wait();
                if (exception)
                    std::rethrow_exception(exception);
                children.back()->readSuffix();
                started = false;
            }
        }


        /** Wait for the data to be ready no more than the specified timeout.
          * Start receiving data if necessary.
          * If the function returned true - the data is ready and you can do `read()`;
          * You can not call the function just at the same moment again.
          *
          * 等待数据准备就绪（等待事件发出信号. 如果事件在指定的时间间隔内发出信号, 则返回true; 否则返回false）
          * 如果该时间大于指定的超时时间, 当前线程返回false, 不接收数据。
          * 如果该时间小于指定的超时时间, 当前线程返回true, 开始接收数据。
          *
          * 如果poll()函数返回true, 表示数据已准备好, 您可以执行read()方法.
          * 不能在同一时刻再次调用函数
          */
        bool poll(UInt64 milliseconds) {
            if (!started) {
                next();
                started = true;
            }

            // 调用tryWait()阻塞当前线程milliseconds.
            // 等待事件发出信号. 如果事件在指定的时间间隔内发出信号, 则返回true; 否则返回false
            return ready.tryWait(milliseconds);
        }


        Block getHeader() const override { return children.at(0)->getHeader(); }


        ~AsynchronousBlockInputStream() override {
            if (started)
                pool.wait();
        }

    protected:
        ThreadPool pool{1};// 声明线程池
        Poco::Event ready;
        bool started = false;
        bool first = true;

        Block block;
        std::exception_ptr exception;

        Block readImpl() override;

        void next();

        /// Calculations that can be performed in a separate thread
        void calculate();
    };

}

