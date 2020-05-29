#pragma once

#include <Common/RWLock.h>

namespace DB
{

/// Structs that hold table structure (columns, their types, default values etc.) locks when executing queries.
/// See IStorage::lock* methods for comments.
//持有表结构的锁, 使表结构在执行查询期间, 列、类型、默认值等不发生变化

struct TableStructureWriteLockHolder
{
    void release()
    {
        *this = TableStructureWriteLockHolder();
    }

private:
    friend class IStorage;

    /// Order is important.
    RWLockImpl::LockHolder alter_intention_lock;
    RWLockImpl::LockHolder new_data_structure_lock;
    RWLockImpl::LockHolder structure_lock;
};

struct TableStructureReadLockHolder
{
private:
    friend class IStorage;

    /// Order is important.
    RWLockImpl::LockHolder new_data_structure_lock;
    RWLockImpl::LockHolder structure_lock;
};

}
