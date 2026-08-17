#include <ripple/pipeline.hpp>

namespace ripple {

void Pipeline::run() {
    runner_->run();
}

} // namespace ripple
