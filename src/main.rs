use tracing::{info, warn, error, Level};
use tracing_subscriber::FmtSubscriber;
use tokio::net::TcpListener;
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

            // 3. --- 🛡️ SİBER GÜVENLİK KALKANI (DLP FILTER) BAŞLIYOR ---
            info!("🛡️ [SEC-OPS] Inspecting payload for forbidden patterns...");
            
            if let Ok(parsed_body) = serde_json::from_slice::<ChatCompletionRequest>(&body_bytes) {
                let forbidden_words = ["kredi kartı", "şifre", "sifre", "api_key", "gizli_veri", "password", "ssn", "tc kimlik"];
                let mut is_breached = false;
                
                for message in parsed_body.messages.iter() {
                    if let Some(content) = message.get("content").and_then(|c| c.as_str()) {
                        let content_lower = content.to_lowercase(); 
                        
                        for &word in &forbidden_words {
                            if content_lower.contains(word) {
                                warn!("🚨 [DLP ALERT] Forbidden keyword detected: '{}'", word);
                                is_breached = true;
                                break;
                            }
                        }
                    }
                    if is_breached { break; }
                }

                if is_breached {
                    warn!("🛑 Agent {} blocked due to security violation!", agent_id);
                    return boxed_error_response(StatusCode::FORBIDDEN, "403 Forbidden: Security Breach Detected (DLP Violation)");
                }
                
                info!("✅ [SEC-OPS] Payload is clean. Proceeding...");
            } else {
                warn!("⚠️ Could not parse JSON for DLP inspection. Proceeding with caution.");
            }
            // --- 🛡️ SİBER GÜVENLİK KALKANI BİTTİ ---

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
                    
                    let mapped_stream = stream! {
                        let mut res_stream = res.bytes_stream();
                        let mut chunk_counter = 0;
                        
                        while let Some(chunk_result) = res_stream.next().await {
                            match chunk_result {
                                Ok(bytes) => {
                                    let chunk_str = String::from_utf8_lossy(&bytes);
                                    chunk_counter += 1; 
                                    
                                    if chunk_str.contains("[DONE]") || chunk_str.contains("DONE") {
                                        info!("🔎 [X-RAY] Final Chunk Detected! Stream ended.");
                                        
                                        let mut r_conn = redis_conn_clone.clone();
                                        let a_id = agent_id_clone.clone();
                                        let deduct_amount = chunk_counter;
                                        
                                        tokio::spawn(async move {
                                            let q_key = format!("quota:{}", a_id);
                                            let new_quota: isize = r_conn.decr(&q_key, deduct_amount).await.unwrap_or(0);
                                            info!("💰 Billing Complete. Agent: {} | Deducted (Chunks): {} | Remaining Quota: {}", a_id, deduct_amount, new_quota);
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

    let redis_client = redis::Client::open("redis://127.0.0.1:6379/")?;
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