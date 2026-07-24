<div align="center">

# Elura

**面向权威实时玩法与可扩展在线游戏服务的开源模块化 Rust 框架。**

[![CI](https://github.com/Arion-Dsh/elura/actions/workflows/ci.yml/badge.svg)](https://github.com/Arion-Dsh/elura/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/elura.svg)](https://crates.io/crates/elura)
[![Rust 1.97+](https://img.shields.io/badge/rust-1.97%2B-dea584.svg?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#许可证)

[文档](https://elura.rustyspottedcat.dev/) · [API 参考](https://docs.rs/elura) · [Crates.io](https://crates.io/crates/elura) · [English](README.md)

</div>

Elura 是一个面向权威实时玩法与可扩展在线游戏服务的开源模块化 Rust 框架。它将客户端连接
与游戏逻辑分离：Gateway 进程负责连接和会话，World 进程执行指令并管理玩家状态。两者可以
独立扩展，也可以组合成单体进程运行。

> [!IMPORTANT]
> `0.3.x` API 已冻结。补丁版本保持公开 API 兼容；破坏性变更只会进入 `0.4.0`。

## 架构

```text
客户端 ── TCP / UDP / WebSocket / WebTransport / QUIC ──▶ Gateway
                                                               │
                                                            指令路由
                                                               │
                                                               ▼
                                                            World
                                                         权威游戏逻辑
```

Elura 提供服务端基础组件，不规定具体的产品规则或数据模型。它不是客户端游戏引擎、托管式
后端服务，也不是游戏服务器实例编排器。

## 功能

- 支持 TCP、UDP、WebSocket、WebTransport 和 QUIC。
- 支持分布式 Gateway/World 部署，也支持单进程模式。
- 提供房间、固定步长模拟、AOI、状态复制、预测和延迟补偿。
- 提供 HTTP 认证与一次性 ELR2 Session ticket 交换。
- 可选 Redis、SQL、Kubernetes、身份认证、通知、OTP 和支付集成。

## 适用场景

Elura 适合：

- 希望使用 Rust 编写权威服务器和游戏逻辑。
- 需要在一套模块化技术栈中组合实时会话、房间、模拟、AOI、状态复制、预测或延迟补偿。
- 希望实时玩法、社交系统、竞技系统、身份和商业服务共享同一个可扩展框架，同时保留
  游戏自定义规则和数据模型。
- 需要独立扩展负责连接的 Gateway 和负责游戏逻辑的 World。
- 应用需要自主控制基础设施、持久化、身份策略和部署方式。

以下情况可以考虑其他方案：

- 希望完全使用托管后端，不自行维护服务器。
- 希望直接获得固定、开箱即用的社交关系、匹配、排行榜、锦标赛和管理 API 与数据模型，
  不准备实现游戏自己的规则和持久化。
- 当前必须使用稳定的 `1.x` API，无法接受 Elura `0.x` 阶段的接口变化。

## 快速开始

安装 CLI 并生成应用：

```bash
cargo install elura-cli --version 0.3.1
elura init all --dir .
```

也可以直接添加 Elura：

```toml
[dependencies]
elura = "0.3.1"
```

概念、配置、crate feature、部署和教程请参阅[项目文档](https://elura.rustyspottedcat.dev/)。

Facade 将契约放在消费它的运行时或能力模块中：

- `elura::gateway` 管理连接、服务发现、在线状态、会话和 ticket API。
- `elura::world` 管理路由、中间件、场景、玩家状态和 World 注册。
- `elura::outbox`、`elura::push`、`elura::ownership`、`elura::replay_protection` 和
  `elura::providers` 管理横切服务。
- `elura::gameplay` 汇总房间、AOI、模拟、网络同步、复制和测试原语。
- `elura::prelude` 只保留常用游戏业务类型的便利导入。

对应的子 crate 采用相同所有权：Gateway 在线状态与服务发现端口由 `elura-gateway`
声明，World 注册端口由 `elura-world` 声明，`elura-core` 只保留双方共享的线协议与领域契约。

## 示例

- [`tiny-network-game`](examples/tiny-network-game)：权威多人移动示例。
- [`realtime-gameplay`](examples/realtime-gameplay)：与传输无关的实时玩法流水线。

## 开发

需要 Rust `1.97` 或更高版本。

```bash
make verify
```

## 许可证

Elura 采用双重许可证，可任选其一：

- [Apache License 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)
