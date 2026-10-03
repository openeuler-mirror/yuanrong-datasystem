// JfClient resilience tests against the repo's mock_jf_server.py:
//   1. a registry-side deletion (heartbeat 404) triggers an automatic
//      re-register without a process restart;
//   2. an empty discovery result maps to K_NOT_FOUND so worker startup can
//      tell "registry briefly empty" apart from transport errors;
//   3. after a registry outage long enough to exhaust the 3-failed-rounds
//      rule, the client recovers once the registry is back.
// Spawns python3 (mock_jf_server.py) and needs the real datasystem SDK for
// the Status implementation; built only when -DDATASYSTEM_SDK_DIR is set.

#include "test_harness.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/jf_service_discovery.h"
#include "vendor/httplib.h"
#include "vendor/nlohmann_json.hpp"

using json = nlohmann::json;
using kvtest::JfClient;
using kvtest::UserCoordinatorDiscovery;

namespace {

constexpr int MOCK_TTL_SEC = 3;  // heartbeat interval = max(ttl / 6, 1) = 1s
// The mock records the peer address of the HTTP connection, so on a
// loopback test the registered address is always 127.0.0.1:<port>.
const std::string LOOPBACK = "127.0.0.1";

int FreePort()
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = 0;
    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    sockaddr_in bound{};
    socklen_t len = sizeof(bound);
    getsockname(fd, reinterpret_cast<sockaddr *>(&bound), &len);
    int port = ntohs(bound.sin_port);
    close(fd);
    return port;
}

// All raw HTTP helpers must carry explicit timeouts: an untimed httplib
// call against a registry that dies mid-request blocks forever and turns
// the whole test binary into a ctest timeout.
std::unique_ptr<httplib::Client> TimedClient(int port)
{
    auto cli = std::make_unique<httplib::Client>(LOOPBACK.c_str(), port);
    cli->set_connection_timeout(2);
    cli->set_read_timeout(5);
    return cli;
}

class MockJfServer {
public:
    ~MockJfServer()
    {
        Stop();
    }

    bool Start()
    {
        port_ = FreePort();
        return port_ > 0 && Spawn();
    }

    // Stop, stay dark for long enough that the client exhausts its
    // failed-rounds budget, then restart on the same port with an empty
    // registry.
    void RestartWithOutage(int outageSeconds)
    {
        Stop();
        std::this_thread::sleep_for(std::chrono::seconds(outageSeconds));
        Spawn();
    }

    void Stop()
    {
        if (pid_ > 0) {
            kill(pid_, SIGKILL);
            waitpid(pid_, nullptr, 0);
            pid_ = -1;
        }
    }

    const std::string &Addr() const
    {
        return addr_;
    }

    int Port() const
    {
        return port_;
    }

private:
    bool Spawn()
    {
        std::string script = std::string(KVTEST_ROOT_CMAKE) + "/src/mock_jf_server.py";
        pid_ = fork();
        if (pid_ == 0) {
            int devnull = open("/dev/null", O_RDONLY);
            dup2(devnull, 0);
            int logFd = open("/dev/null", O_WRONLY);
            dup2(logFd, 1);
            dup2(logFd, 2);
            execlp("python3", "python3", script.c_str(), "--port", std::to_string(port_).c_str(),
                   "--ttl-default", std::to_string(MOCK_TTL_SEC).c_str(), nullptr);
            _exit(127);
        }
        for (int i = 0; i < 100; i++) {
            if (WaitForHealthOnce()) {
                addr_ = LOOPBACK + ":" + std::to_string(port_);
                return true;
            }
            if (waitpid(pid_, nullptr, WNOHANG) == pid_) {
                pid_ = -1;
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    }

    bool WaitForHealthOnce()
    {
        auto cli = TimedClient(port_);
        auto res = cli->Get("/health");
        return res && res->status == 200;
    }

    int port_ = 0;
    pid_t pid_ = -1;
    std::string addr_ = LOOPBACK + ":0";
};

MockJfServer &MockJf()
{
    static MockJfServer server;
    return server;
}

// Redirects stderr into a temp file while alive, so a test can assert on the
// JfClient's diagnostic lines (its heartbeat/re-register reports go to
// stderr). Call Content() after the client's threads are joined.
class StderrCapture {
public:
    StderrCapture() : saved_(dup(fileno(stderr)))
    {
        file_ = std::tmpfile();
        if (file_ != nullptr) {
            dup2(fileno(file_), STDERR_FILENO);
        }
    }

    ~StderrCapture()
    {
        Restore();
        if (file_ != nullptr) {
            fclose(file_);
        }
    }

    std::string Content()
    {
        fflush(stderr);
        Restore();
        std::string out;
        if (file_ == nullptr) {
            return out;
        }
        char buf[4096];
        size_t n;
        rewind(file_);
        while ((n = fread(buf, 1, sizeof(buf), file_)) > 0) {
            out.append(buf, n);
        }
        return out;
    }

private:
    void Restore()
    {
        if (saved_ >= 0) {
            dup2(saved_, STDERR_FILENO);
            close(saved_);
            saved_ = -1;
        }
    }

    int saved_ = -1;
    FILE *file_ = nullptr;
};

bool DiscoverContains(const std::string &service, const std::string &expected)
{
    auto cli = TimedClient(MockJf().Port());
    auto res = cli->Get("/discover/" + service);
    if (!res || res->status != 200) {
        return false;
    }
    try {
        auto j = json::parse(res->body);
        for (auto &addr : j["instances"]) {
            if (addr.get<std::string>() == expected) {
                return true;
            }
        }
    } catch (const std::exception &) {
    }
    return false;
}

bool WaitForInstance(const std::string &service, const std::string &expected, int timeoutSeconds)
{
    for (int i = 0; i < timeoutSeconds * 5; i++) {
        if (DiscoverContains(service, expected)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}

int RegisterEventCount(const std::string &service)
{
    auto cli = TimedClient(MockJf().Port());
    auto res = cli->Get(("/events?service=" + service).c_str());
    if (!res || res->status != 200) {
        return -1;
    }
    try {
        auto events = json::parse(res->body);
        int count = 0;
        for (auto &event : events) {
            if (event.value("action", "") == "register") {
                count++;
            }
        }
        return count;
    } catch (const std::exception &) {
        return -1;
    }
}

// Remove the instance from the registry behind the client's back, which is
// what the mock's TTL sweeper does to a stalled coordinator.
bool DropInstance(const std::string &service, int port)
{
    auto cli = TimedClient(MockJf().Port());
    json body = { { "service", service }, { "port", port } };
    auto res = cli->Post("/unregister", body.dump(), "application/json");
    return res && res->status == 200;
}

}  // namespace

TEST(discovery_empty_returns_not_found)
{
    auto client = std::make_shared<JfClient>(MockJf().Addr(), MOCK_TTL_SEC);
    UserCoordinatorDiscovery discovery(client, "no_such_service");
    std::vector<std::string> out;
    auto rc = discovery.GetCoordinators(out);
    ASSERT_TRUE(rc.IsError());
    ASSERT_TRUE(rc.GetCode() == datasystem::StatusCode::K_NOT_FOUND);

    ASSERT_TRUE(client->RegisterService("ut_found", 47002).IsOk());
    UserCoordinatorDiscovery found(client, "ut_found");
    out.clear();
    rc = found.GetCoordinators(out);
    ASSERT_TRUE(rc.IsOk());
    ASSERT_EQ(out.size(), 1UL);
    client->UnregisterService("ut_found", 47002);
}

TEST(heartbeat_404_reregisters)
{
    JfClient client(MockJf().Addr(), MOCK_TTL_SEC);
    ASSERT_TRUE(client.RegisterService("ut_404", 47001).IsOk());
    const std::string expected = LOOPBACK + ":47001";
    ASSERT_TRUE(WaitForInstance("ut_404", expected, 5));

    ASSERT_TRUE(DropInstance("ut_404", 47001));
    // Re-register must happen on the next heartbeat round(s), no restart.
    ASSERT_TRUE(WaitForInstance("ut_404", expected, 10));
    ASSERT_TRUE(RegisterEventCount("ut_404") >= 2);

    client.UnregisterService("ut_404", 47001);
}

TEST(registry_outage_recovers)
{
    // Capture stderr so the 3-failed-rounds branch is actually asserted:
    // with the mock down, "registration lost" + "re-register failed" can
    // only come from that branch (no 404 is possible while it is down).
    StderrCapture capture;
    JfClient client(MockJf().Addr(), MOCK_TTL_SEC);
    ASSERT_TRUE(client.RegisterService("ut_outage", 47003).IsOk());
    ASSERT_TRUE(WaitForInstance("ut_outage", LOOPBACK + ":47003", 5));

    // 8s outage: with a 1s interval every round fails (attempt + 1s retry),
    // so the 3-failed-rounds re-register rule fires while the registry is
    // still down and fails; recovery must still happen once it is back.
    MockJf().RestartWithOutage(8);
    ASSERT_TRUE(WaitForInstance("ut_outage", LOOPBACK + ":47003", 20));
    // The restart gave the mock a fresh registry, so the only register event
    // it has ever seen is the recovery one; >= 1 proves the client healed.
    ASSERT_TRUE(RegisterEventCount("ut_outage") >= 1);

    client.UnregisterService("ut_outage", 47003);
    const std::string stderrLog = capture.Content();
    ASSERT_TRUE(stderrLog.find("JF registration lost for ut_outage:47003") != std::string::npos);
    ASSERT_TRUE(stderrLog.find("JF re-register failed for ut_outage:47003") != std::string::npos);
    ASSERT_TRUE(stderrLog.find("JF re-register succeeded for ut_outage:47003") != std::string::npos);
}

TEST(unregister_then_register_restarts_heartbeat)
{
    JfClient client(MockJf().Addr(), MOCK_TTL_SEC);
    ASSERT_TRUE(client.RegisterService("ut_restart", 47004).IsOk());
    ASSERT_TRUE(WaitForInstance("ut_restart", LOOPBACK + ":47004", 5));
    ASSERT_TRUE(client.UnregisterService("ut_restart", 47004).IsOk());

    // Register again on the same client: a per-key stop must not disable the
    // client. Surviving past the TTL proves the new heartbeat thread runs
    // (without heartbeats the sweeper expires the instance at 3s).
    ASSERT_TRUE(client.RegisterService("ut_restart", 47004).IsOk());
    std::this_thread::sleep_for(std::chrono::seconds(MOCK_TTL_SEC + 3));
    ASSERT_TRUE(DiscoverContains("ut_restart", LOOPBACK + ":47004"));
    ASSERT_TRUE(RegisterEventCount("ut_restart") >= 2);

    client.UnregisterService("ut_restart", 47004);
}

// Regression for the in-flight-recovery race: while the heartbeat thread is
// blocked inside a re-register HTTP call, UnregisterService runs. Unregister
// must stay terminal -- after it returns, no further register/heartbeat may
// arrive (the old code spawned a replacement heartbeat thread here).
TEST(unregister_is_terminal_despite_inflight_reregister)
{
    constexpr int RACE_PORT = 47005;
    std::mutex gateMutex;
    std::condition_variable gateCv;
    bool gateOpen = true;
    std::atomic<int> registers{ 0 };
    std::atomic<int> heartbeats{ 0 };
    std::atomic<int> unregisters{ 0 };
    std::atomic<bool> instancePresent{ false };

    httplib::Server svr;
    svr.Post("/register", [&](const httplib::Request &, httplib::Response &res) {
        registers.fetch_add(1);
        std::unique_lock<std::mutex> lk(gateMutex);
        gateCv.wait(lk, [&] { return gateOpen; });
        instancePresent.store(true);
        res.status = 200;
        res.set_content("{\"ok\":true}", "application/json");
    });
    svr.Post("/heartbeat", [&](const httplib::Request &, httplib::Response &res) {
        heartbeats.fetch_add(1);
        res.status = instancePresent.load() ? 200 : 404;
        res.set_content(instancePresent.load() ? "{\"ok\":true}" : "{\"ok\":false}", "application/json");
    });
    svr.Post("/unregister", [&](const httplib::Request &, httplib::Response &res) {
        unregisters.fetch_add(1);
        instancePresent.store(false);
        res.status = 200;
        res.set_content("{\"ok\":true}", "application/json");
    });
    std::thread serverThread([&] { svr.listen("127.0.0.1", RACE_PORT); });
    for (int i = 0; i < 50 && !svr.is_running(); i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_TRUE(svr.is_running());

    auto setGate = [&](bool open) {
        {
            std::lock_guard<std::mutex> lk(gateMutex);
            gateOpen = open;
        }
        gateCv.notify_all();
    };

    JfClient client(LOOPBACK + ":" + std::to_string(RACE_PORT), MOCK_TTL_SEC);
    ASSERT_TRUE(client.RegisterService("ut_race", RACE_PORT).IsOk());
    for (int i = 0; i < 50 && registers.load() < 1; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_EQ(registers.load(), 1);

    setGate(false);
    instancePresent.store(false);  // registry-side removal -> 404 -> ReRegister blocks on the gate
    for (int i = 0; i < 50 && registers.load() < 2; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_EQ(registers.load(), 2);  // in-flight re-register is parked in the handler

    std::thread unregistering([&] { client.UnregisterService("ut_race", RACE_PORT); });
    // Give StopHeartbeat time to erase the key and park in join, then let
    // the parked register through: with the fix the thread exits instead of
    // being replaced by a new heartbeat.
    std::this_thread::sleep_for(std::chrono::seconds(1));
    setGate(true);
    unregistering.join();

    const int registersAtReturn = registers.load();
    const int heartbeatsAtReturn = heartbeats.load();
    std::this_thread::sleep_for(std::chrono::seconds(MOCK_TTL_SEC + 2));
    ASSERT_EQ(unregisters.load(), 1);
    ASSERT_EQ(registers.load(), registersAtReturn);
    ASSERT_EQ(heartbeats.load(), heartbeatsAtReturn);

    svr.stop();
    serverThread.join();
}

int main()
{
    if (!MockJf().Start()) {
        printf("mock JF server failed to start (python3 required)\n");
        return 1;
    }
    RUN_TESTS();
}
