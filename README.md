# 🛡️ KalkanAI

**Enterprise-Grade Zero-Trust AI Gateway**

KalkanAI is an ultra-low latency, Rust-based asynchronous proxy engine designed to secure, monitor, and financially manage autonomous AI agents connecting to Large Language Models (LLMs) like OpenAI, Gemini, and Ollama.

---

## 🚀 Core Architecture & Features

KalkanAI sits between your internal network of AI agents and external/internal LLM providers, acting as a relentless gatekeeper.

### 💰 1. FinOps: Real-Time AI Billing & Quota Management
* **Zero Double-Spending:** Atomic budget deductions using Redis Lua scripts.
* **Stream X-Ray:** Intercepts Server-Sent Events (SSE) on-the-fly to calculate exact token usage (Prompt + Completion) without breaking the stream.
* **Pre-paid Agent Wallets:** Blocks requests instantly (`429 Too Many Requests`) if an agent depletes its token budget.

### 🛑 2. SecOps: Pre-Flight Data Loss Prevention (DLP)
* **Zero-Trust Payload Inspection:** Unpacks JSON payloads in microseconds before they reach the LLM.
* **Rogue Agent Defense:** Detects forbidden keywords (e.g., passwords, API keys, PII, credit card numbers).
* **Instant Kill-Switch:** Drops the connection (`403 Forbidden`) before any sensitive data leaks to the external provider.

### ⚡ 3. Engine Physics
* **Language:** Built entirely in **Rust** (`hyper`, `tokio`).
* **Zero-Copy Streaming:** Passes LLM streams to clients without memory allocation overhead.
* **State Store:** Asynchronous connection pooling via **Redis**.

---

## 🛠️ Usage

### Starting the Engine
Make sure Redis is running (`redis://127.0.0.1:6379/`), then ignite the Rust engine:
```bash
cargo run