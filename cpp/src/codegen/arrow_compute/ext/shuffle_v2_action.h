#pragma once

#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <arrow/util/checked_cast.h>
#include <iostream>
#include <memory>
#include <sstream>

namespace sparkcolumnarplugin {
namespace codegen {
namespace arrowcompute {
namespace extra {
using ArrayList = std::vector<std::shared_ptr<arrow::Array>>;
//////////////// ShuffleV2Action ///////////////
class ShuffleV2Action {
 public:
  ShuffleV2Action(arrow::compute::FunctionContext* ctx,
                  std::shared_ptr<arrow::DataType> type, int id);
  ~ShuffleV2Action();
  arrow::Status Submit(std::vector<std::function<bool()>> is_null_func_list,
                       std::vector<std::function<void*()>> get_func_list,
                       std::function<arrow::Status()>* func);
  arrow::Status FinishAndReset(ArrayList* out);
  class Impl;

 private:
  std::shared_ptr<Impl> impl_;
};

///////////////////// Public Functions //////////////////
static arrow::Status MakeShuffleV2Action(arrow::compute::FunctionContext* ctx,
                                         std::shared_ptr<arrow::DataType> type, int id,
                                         std::shared_ptr<ShuffleV2Action>* out) {
  *out = std::make_shared<ShuffleV2Action>(ctx, type, id);
  return arrow::Status::OK();
}
}  // namespace extra
}  // namespace arrowcompute
}  // namespace codegen
}  // namespace sparkcolumnarplugin
