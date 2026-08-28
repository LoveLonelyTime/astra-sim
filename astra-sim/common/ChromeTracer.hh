#ifndef __COMMON_CHROME_TRACER_HH__
#define __COMMON_CHROME_TRACER_HH__

#include <string>
#include <vector>
#include <memory>
#include <mutex>

#define CHROME_TRACER_CAT_CPU_OP "CPU_OP"
#define CHROME_TRACER_CAT_GPU_OP "GPU_OP"
#define CHROME_TRACER_CAT_MEM_OP "MEM_OP"
#define CHROME_TRACER_CAT_COMM_OP "COMM_OP"

namespace AstraSim {

class ChromeTracer {
public:
    ChromeTracer(const ChromeTracer&) = delete;
    ChromeTracer& operator=(const ChromeTracer&) = delete;

    static ChromeTracer& GetInstance() {
        static ChromeTracer instance;
        return instance;
    }

    struct TraceEvent {
        std::string name;
        std::string cat;
        uint64_t ts;
        uint64_t dur;
        uint32_t pid;
        uint32_t tid;
    };

    std::unique_ptr<TraceEvent> Begin(const std::string& name, const std::string& cat, uint64_t start, uint32_t instance_id, uint32_t npu_id);
    void End(std::unique_ptr<TraceEvent> event, uint64_t end);

    void Dump(const std::string& file_name);
    void Clear();

private:
    ChromeTracer() = default;
    ~ChromeTracer() = default;

    std::vector<std::unique_ptr<TraceEvent>> m_events;
    std::mutex m_mutex;
};

}  // namespace AstraSim

#endif