# MemV

独立的 AML 记忆接口，代码和使用说明见 [aml/README.md](aml/README.md)。

在 `aml/server.py` 将自己的 `MemoryBackend` 实例传入 `create_app` 即可。
当前不绑定任何具体记忆算法；未配置 backend 时接口明确返回 503。

旧代码和数据在此次整理时已完整备份至相邻的 `MemV-memmachine-backup.*` 目录。
`aml/data/` 中原有数据也保留；本接口层只管理请求回执。
