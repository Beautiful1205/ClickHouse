#pragma once

#include <DataStreams/IBlockInputStream.h>

namespace DB
{

/** Converts columns-constants to full columns ("materializes" them).
  * 将列常量转换为完整列("物化"常量列)
  */
class MaterializingBlockInputStream : public IBlockInputStream
{
public:
    MaterializingBlockInputStream(const BlockInputStreamPtr & input);
    String getName() const override;
    Block getHeader() const override;

protected:
    Block readImpl() override;
};

}
