#include "astra-sim/common/ChromeTracer.hh"
#include <fstream>

namespace AstraSim {

std::unique_ptr<ChromeTracer::TraceEvent> ChromeTracer::Begin(const std::string& name, const std::string& cat, uint64_t start, uint32_t instance_id, uint32_t npu_id) {
    auto event = std::make_unique<ChromeTracer::TraceEvent>();
    event->name = name;
    event->cat = cat;
    event->ts = start;
    event->dur = 0;
    event->pid = instance_id;
    event->tid = npu_id;

    return event;
}

void ChromeTracer::End(std::unique_ptr<ChromeTracer::TraceEvent> event, uint64_t end) {
    event->dur = end - event->ts;

    std::lock_guard<std::mutex> lock(m_mutex);
    m_events.push_back(std::move(event));
}

void ChromeTracer::Dump(const std::string& file_name)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    std::ofstream ofs(file_name);

    if (!ofs.is_open()) {
        throw std::runtime_error("Failed to open trace file: " + file_name);
    }

    ofs << "{\n";
    ofs << "  \"traceEvents\": [\n";

    for (size_t i = 0; i < m_events.size(); ++i) {
        const auto& event = m_events[i];

        ofs << "    {\n";
        ofs << "      \"name\": \"" << event->name << "\",\n";
        ofs << "      \"cat\": \"" << event->cat << "\",\n";
        ofs << "      \"ph\": \"X\",\n";
        ofs << "      \"ts\": " << event->ts << ",\n";
        ofs << "      \"dur\": " << event->dur << ",\n";
        ofs << "      \"pid\": " << event->pid << ",\n";
        ofs << "      \"tid\": " << event->tid << "\n";
        ofs << "    }";

        if (i + 1 != m_events.size()) {
            ofs << ",";
        }

        ofs << "\n";
    }

    ofs << "  ]\n";
    ofs << "}\n";

    m_events.clear();
}

void ChromeTracer::Clear() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_events.clear();
}

}  // namespace AstraSim