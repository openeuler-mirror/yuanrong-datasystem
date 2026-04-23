{
  global:
    extern "C++" {
      datasystem::*;
      vtable?for?datasystem::*;
      typeinfo?for?datasystem::*;
      typeinfo?name?for?datasystem::*;
    };
    _ZTI*;        # 显式导出所有 RTTI 符号
    _ZTV*;        # 显式导出所有虚表符号（如果 Buffer 有虚函数）
  local:
    *;
};


