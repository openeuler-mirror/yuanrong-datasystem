{
  global:
    extern "C++" {
      datasystem::*;
      vtable?for?datasystem::*;
      typeinfo?for?datasystem::*;
      typeinfo?name?for?datasystem::*;
      # brpc/butil symbols required by dynamic_deps consumers (libds_client_py.so)
      logging::*;
      # absl symbols required in debug builds (LOG(FATAL)/CHECK use absl internals)
      absl::*;
    };
    _ZTI*;        # 显式导出所有 RTTI 符号
    _ZTV*;        # 显式导出所有虚表符号（如果 Buffer 有虚函数）
    _ZN10datasystem*;  # Fallback: export all datasystem::* symbols via raw mangled names.
                       # C++ demangling in version scripts can miss data symbols (e.g.
                       # FLAGS_use_brpc) in some linker versions.  This mangled-name
                       # pattern is redundant with the extern "C++" block above and
                       # serves as a safety net.
  local:
    *;
};


