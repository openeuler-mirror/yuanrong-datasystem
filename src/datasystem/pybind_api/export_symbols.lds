{
  global:
    PyInit_*;     # 导出所有 PyInit_ 开头的符号
    *pybind11*;   # 导出 pybind11 相关符号（可选）
  local:
    *;            # 其他所有符号隐藏
};
