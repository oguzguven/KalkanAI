use tracing::{info, warn, error, Level};
use tracing_subscriber::FmtSubscriber;
use tokio::net::TcpListener;
mod cache;
use tokio::signal;
use std::net::SocketAddr;

// Hyper & Body
use hyper::{Request, Response, StatusCode, Method};
use hyper::body::{Incoming, Frame}; 
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use http_body_util::{BodyExt, Full, StreamBody, combinators::BoxBody};
use serde::{Deserialize, Serialize};
use bytes::Bytes; 

// Streaming & Asenkron Araçlar
use futures_util::StreamExt;
use async_stream::stream; 
use reqwest::Client;

// Redis
use redis::AsyncCommands;

#[derive(Deserialize, Serialize, Debug)]
struct ChatCompletionRequest {
    model: String,
    stream: Option<bool>, 
    messages: Vec<serde_json::Value>,
}

fn boxed_error_response(status: StatusCode, message: &'static str) -> Result<Response<BoxBody<Bytes, std::io::Error>>, hyper::Error> {
    let body = BodyExt::boxed(Full::new(Bytes::from(message)).map_err(|e| match e {}));
    
    Ok(Response::builder()
        .status(status)
        .body(body)
        .unwrap())
}

async fn proxy_handler(
    req: Request<Incoming>,
    mut redis_conn: redis::aio::ConnectionManager,
) -> Result<Response<BoxBody<Bytes, std::io::Error>>, hyper::Error> {
    
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/v1/chat/completions") => {
            
            // 1. KİMLİK DOĞRULAMA
            let agent_id = match req.headers().get("x-agent-id") {
                Some(id) => id.to_str().unwrap_or_default().to_string(),
                None => {
                    warn!("🚨 Blocked Request: Missing x-agent-id header");
                    return boxed_error_response(StatusCode::UNAUTHORIZED, "Unauthorized: Missing Agent ID");
                }
            };

            // 2. FİNANSAL KONTROL (KOTA)
            let quota_key = format!("quota:{}", agent_id);
            let current_quota: redis::RedisResult<isize> = redis_conn.get(&quota_key).await;

            match current_quota {
                Ok(quota) if quota <= 0 => {
                    warn!("📉 Quota Depleted for Agent: {}", agent_id);
                    return boxed_error_response(StatusCode::TOO_MANY_REQUESTS, "429 Too Many Requests: Quota Exceeded!");
                }
                Ok(_) => info!("✅ Initial Quota Check Passed for Agent: {}", agent_id),
                Err(e) => {
                    error!("🔥 Redis Connection Error: {}", e);
                    return boxed_error_response(StatusCode::INTERNAL_SERVER_ERROR, "Database failure");
                }
            }

            // Gelen isteğin gövdesini okuyoruz
            let body_bytes = match req.collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(_) => return boxed_error_response(StatusCode::BAD_REQUEST, "Failed to read body"),
            };

            // --- 🛡️ SİBER GÜVENLİK KALKANI (DLP FILTER) BAŞLIYOR ---
            info!("🛡️ [SEC-OPS] Inspecting payload dynamically via Redis DLP...");
            
            if let Ok(parsed_body) = serde_json::from_slice::<ChatCompletionRequest>(&body_bytes) {
                
                // YENİ NESİL: Yasaklı kelimeleri koda gömülü diziden değil, anlık olarak Redis'ten çekiyoruz!
                let mut r_conn_dlp = redis_conn.clone();
                let forbidden_words: Vec<String> = r_conn_dlp.smembers("dlp:blacklist").await.unwrap_or_default();
                
                let mut is_breached = false;
                
                if !forbidden_words.is_empty() {
                    for message in parsed_body.messages.iter() {
                        if let Some(content) = message.get("content").and_then(|c| c.as_str()) {
                            let content_lower = content.to_lowercase(); 
                            
                            for word in &forbidden_words {
                                if content_lower.contains(word) {
                                    warn!("🚨 [DLP ALERT] Forbidden keyword detected via Redis: '{}'", word);
                                    is_breached = true;
                                    break;
                                }
                            }
                        }
                        if is_breached { break; }
                    }
                } else {
                    warn!("⚠️ DLP Blacklist is empty or Redis is unreachable. Proceeding without DLP.");
                }

                // Yasaklı kelime bulunduysa tetiği çek ve isteği blokla!
                if is_breached {
                    warn!("🛑 Agent {} blocked due to dynamic security violation!", agent_id);
                    return boxed_error_response(StatusCode::FORBIDDEN, "403 Forbidden: Security Breach Detected (Dynamic DLP Violation)");
                }
                
                info!("✅ [SEC-OPS] Payload is clean. Proceeding...");
            } else {
                warn!("⚠️ Could not parse JSON for DLP inspection. Proceeding with caution.");
            }
            // --- 🛡️ SİBER GÜVENLİK KALKANI BİTTİ ---

            // --- 🧠 SEMANTİK ÖNBELLEK (AI CACHING) - FAZ 1 (OKUMA) BAŞLIYOR ---
            info!("🧠 [CACHING] Checking semantic memory for the prompt...");

            let mut extracted_prompt = String::new();
            
            // NEDEN SADECE SON MESAJ?: Genellikle LLM'e giden isteklerde asıl maliyetli soru son `user` mesajıdır.
            if let Ok(parsed_body) = serde_json::from_slice::<ChatCompletionRequest>(&body_bytes) {
                if let Some(last_message) = parsed_body.messages.last() {
                    if let Some(content) = last_message.get("content").and_then(|c| c.as_str()) {
                        extracted_prompt = content.to_string();
                    }
                }
            }

            let mut current_cache_key = String::new();

            if !extracted_prompt.is_empty() {
                // SoC: Şifreleme işini ana fonksiyonda değil, modülde yapıyoruz.
                current_cache_key = cache::generate_cache_key(&extracted_prompt);
                let mut r_conn_cache = redis_conn.clone();
                
                if let Some(cached_response) = cache::check_cache(&mut r_conn_cache, &current_cache_key).await {
                    // 🎯 CACHE HIT DURUMU: LLM faturası sıfırlandı!
                    let simulated_chunk = format!(
                        "data: {{\"id\":\"cache-hit\",\"object\":\"chat.completion.chunk\",\"model\":\"kalkan-cached\",\"choices\":[{{\"delta\":{{\"role\":\"assistant\",\"content\":{}}},\"finish_reason\":\"stop\"}}]}}\n\ndata: [DONE]\n\n",
                        serde_json::to_string(&cached_response).unwrap_or_default()
                    );

                    let stream_body = BodyExt::boxed(Full::new(Bytes::from(simulated_chunk)).map_err(|e| match e {}));
                    
                    info!("🚀 [CACHING] Bypassing Upstream. Serving directly from Redis Memory!");
                    
                    return Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "text/event-stream")
                        .body(stream_body)
                        .unwrap());
                }
            } else {
                warn!("⚠️ Could not extract prompt for caching.");
            }
            // --- 🧠 SEMANTİK ÖNBELLEK BİTTİ ---

            // 4. OLLAMA'YA (UPSTREAM) YÖNLENDİRME
            let upstream_url = std::env::var("UPSTREAM_URL")
                .unwrap_or_else(|_| "http://localhost:11434/v1/chat/completions".to_string());
            
            info!("🚀 Forwarding Stream to Upstream Engine...");
            let client = Client::new();
            
            let upstream_res = client.post(&upstream_url)
                .header("Content-Type", "application/json")
                .body(body_bytes.to_vec()) 
                .send()
                .await;

            match upstream_res {
                Ok(res) => {
                    if !res.status().is_success() {
                        return boxed_error_response(StatusCode::BAD_GATEWAY, "Upstream returned an error");
                    }

                    let agent_id_clone = agent_id.clone();
                    let redis_conn_clone = redis_conn.clone();
                    let cache_key_clone = current_cache_key.clone(); // 🎯 YENİ: Hafıza anahtarı
                    
                    let mapped_stream = stream! {
                        let mut res_stream = res.bytes_stream();
                        let mut chunk_counter = 0;
                        let mut full_response = String::new(); // 🎯 YENİ: Kelime deposu
                        
                        while let Some(chunk_result) = res_stream.next().await {
                            match chunk_result {
                                Ok(bytes) => {
                                    let chunk_str = String::from_utf8_lossy(&bytes);
                                    chunk_counter += 1; 
                                    
                                    // --- 🧠 SEMANTİK ÖNBELLEK TOPLAYICISI (COLLECTOR) ---
                                    for line in chunk_str.lines() {
                                        if line.starts_with("data: ") && !line.contains("[DONE]") {
                                            let json_str = &line[6..];
                                            if let Ok(parsed_chunk) = serde_json::from_str::<serde_json::Value>(json_str) {
                                                if let Some(content) = parsed_chunk["choices"][0]["delta"]["content"].as_str() {
                                                    full_response.push_str(content);
                                                }
                                            }
                                        }
                                    }
                                    // ----------------------------------------------------
                                    
                                    if chunk_str.contains("[DONE]") || chunk_str.contains("DONE") {
                                        info!("🔎 [X-RAY] Final Chunk Detected! Stream ended.");
                                        
                                        let mut r_conn = redis_conn_clone.clone();
                                        let a_id = agent_id_clone.clone();
                                        let deduct_amount = chunk_counter;
                                        let c_key = cache_key_clone.clone();
                                        let final_resp = full_response.clone();
                                        
                                        tokio::spawn(async move {
                                            let q_key = format!("quota:{}", a_id);
                                            let new_quota: isize = r_conn.decr(&q_key, deduct_amount).await.unwrap_or(0);
                                            info!("💰 Billing Complete. Agent: {} | Deducted: {} | Remaining: {}", a_id, deduct_amount, new_quota);
                                            
                                            // 💾 Hafızaya Yazma İşlemi (24 Saat TTL)
                                            if !c_key.is_empty() && !final_resp.is_empty() {
                                                let _: redis::RedisResult<()> = r_conn.set_ex(&c_key, final_resp, 86400).await;
                                                info!("💾 [CACHING] Response successfully saved to Redis! Key: {}", c_key);
                                            }
                                        });
                                    }
                                    
                                    yield Ok::<Frame<Bytes>, std::io::Error>(Frame::data(bytes));
                                },
                                Err(e) => {
                                    error!("Stream error: {}", e);
                                    yield Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()));
                                }
                            }
                        }
                    };

                    let stream_body = BodyExt::boxed(StreamBody::new(mapped_stream));
                    
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "text/event-stream")
                        .body(stream_body)
                        .unwrap())
                }
                Err(err) => {
                    error!("💥 Upstream Connection Failed: {}", err);
                    boxed_error_response(StatusCode::BAD_GATEWAY, "502 Bad Gateway: LLM Upstream Unreachable")
                }
            }
        }
        _ => boxed_error_response(StatusCode::NOT_FOUND, "Endpoint Not Found"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = FmtSubscriber::builder().with_max_level(Level::INFO).finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("🚀 KalkanAI Asynchronous Engine Initiated...");

// YENİ: Redis adresini dışarıdan (Çevresel Değişken) alıyoruz. Bulamazsa 127.0.0.1 kullanıyor.
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let redis_client = redis::Client::open(redis_url)?;
    let redis_conn = redis::aio::ConnectionManager::new(redis_client).await?;
    info!("🟢 Redis Connection Established!");

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = TcpListener::bind(addr).await?;
    info!("⚡ Proxy Engine Online. Listening for STREAMING Traffic on {}", addr);

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                if let Ok((stream, peer_addr)) = accept_result {
                    let io = TokioIo::new(stream);
                    let redis_conn_clone = redis_conn.clone();
                    
                    tokio::task::spawn(async move {
                        let service = service_fn(move |req| {
                            proxy_handler(req, redis_conn_clone.clone())
                        });

                        if let Err(err) = http1::Builder::new()
                            .serve_connection(io, service)
                            .await
                        {
                            warn!("Error serving connection from {}: {:?}", peer_addr, err);
                        }
                    });
                }
            }
            _ = signal::ctrl_c() => {
                info!("🛑 Shutdown signal received. Draining streams...");
                break;
            }
        }
    }

    Ok(())
}