/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __INT_DATA_HH__
#define __INT_DATA_HH__

#include "astra-sim/common/ChromeTracer.hh"

namespace AstraSim {

class IntData : public CallData {
  public:
    IntData(int d) {
        data = d;
    }
    int data;
    uint64_t execution_time;
    std::unique_ptr<ChromeTracer::TraceEvent> tracer_event;
};

}  // namespace AstraSim

#endif /* __INT_DATA_HH__ */
