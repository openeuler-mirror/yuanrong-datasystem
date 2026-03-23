#include <memory>
#include <string>
#include <vector>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "datasystem/transfer_engine/status.h"
#include "datasystem/transfer_engine/transfer_engine.h"

namespace py = pybind11;

namespace datasystem {
namespace {

class PyTransferEngine {
public:
    PyTransferEngine()
    {
        engine_ = std::make_unique<TransferEngine>();
    }

    Status Initialize(const std::string &localHostname, const std::string &protocol, const std::string &deviceName)
    {
        return engine_->Initialize(localHostname, protocol, deviceName);
    }

    Status RegisterMemory(uintptr_t bufferAddr, size_t length)
    {
        return engine_->RegisterMemory(bufferAddr, length);
    }

    int32_t GetRpcPort()
    {
        return engine_->GetRpcPort();
    }

    Status BatchRegisterMemory(const std::vector<uintptr_t> &bufferAddrs, const std::vector<size_t> &lengths)
    {
        return engine_->BatchRegisterMemory(bufferAddrs, lengths);
    }

    Status UnregisterMemory(uintptr_t bufferAddr)
    {
        return engine_->UnregisterMemory(bufferAddr);
    }

    Status BatchUnregisterMemory(const std::vector<uintptr_t> &bufferAddrs)
    {
        return engine_->BatchUnregisterMemory(bufferAddrs);
    }

    Status TransferSyncRead(const std::string &targetHostname, uintptr_t buffer, uintptr_t peerBufferAddress,
                            size_t length)
    {
        return engine_->TransferSyncRead(targetHostname, buffer, peerBufferAddress, length);
    }

    Status BatchTransferSyncRead(const std::string &targetHostname, const std::vector<uintptr_t> &buffers,
                                 const std::vector<uintptr_t> &peerBufferAddresses,
                                 const std::vector<size_t> &lengths)
    {
        return engine_->BatchTransferSyncRead(targetHostname, buffers, peerBufferAddresses, lengths);
    }

    Status Finalize()
    {
        return engine_->Finalize();
    }

private:
    std::unique_ptr<TransferEngine> engine_;
};

}  // namespace
}  // namespace datasystem

PYBIND11_MODULE(_transfer_engine, m)
{
    m.doc() = "Python bindings for transfer_engine";

    py::enum_<datasystem::StatusCode>(m, "StatusCode")
        .value("kOk", datasystem::StatusCode::kOk)
        .value("kInvalid", datasystem::StatusCode::kInvalid)
        .value("kNotFound", datasystem::StatusCode::kNotFound)
        .value("kRuntimeError", datasystem::StatusCode::kRuntimeError)
        .value("kNotReady", datasystem::StatusCode::kNotReady)
        .value("kNotAuthorized", datasystem::StatusCode::kNotAuthorized)
        .value("kNotSupported", datasystem::StatusCode::kNotSupported)
        .export_values();

    py::class_<datasystem::Status>(m, "Status")
        .def(py::init<>())
        .def("is_ok", &datasystem::Status::IsOk)
        .def("is_error", &datasystem::Status::IsError)
        .def("get_code", &datasystem::Status::GetCode)
        .def("get_msg", &datasystem::Status::GetMsg)
        .def("to_string", &datasystem::Status::ToString)
        .def("__repr__", [](const datasystem::Status &s) {
            return std::string("Status(") + s.ToString() + ")";
        });

    py::class_<datasystem::PyTransferEngine>(m, "TransferEngine")
        .def(py::init<>())
        .def("initialize", &datasystem::PyTransferEngine::Initialize,
             py::arg("local_hostname"), py::arg("protocol"), py::arg("device_name"))
        .def("get_rpc_port", &datasystem::PyTransferEngine::GetRpcPort)
        .def("register_memory", &datasystem::PyTransferEngine::RegisterMemory,
             py::arg("buffer_addr_regisrterch"), py::arg("length"))
        .def("batch_register_memory", &datasystem::PyTransferEngine::BatchRegisterMemory,
             py::arg("buffer_addrs"), py::arg("lengths"))
        .def("unregister_memory", &datasystem::PyTransferEngine::UnregisterMemory,
             py::arg("buffer_addr_regisrterch"))
        .def("batch_unregister_memory", &datasystem::PyTransferEngine::BatchUnregisterMemory,
             py::arg("buffer_addrs"))
        .def("transfer_sync_read", &datasystem::PyTransferEngine::TransferSyncRead,
             py::arg("target_hostname"), py::arg("buffer"), py::arg("peer_buffer_address"), py::arg("length"))
        .def("batch_transfer_sync_read", &datasystem::PyTransferEngine::BatchTransferSyncRead,
             py::arg("target_hostname"), py::arg("buffers"), py::arg("peer_buffer_addresses"), py::arg("lengths"))
        .def("finalize", &datasystem::PyTransferEngine::Finalize);
}
