use sha2::{Sha256, Digest};
use redis::AsyncCommands;
use tracing::{info, error};

// KODLAMA STANDARDI (Clean Code): Fonksiyonlar tek bir işe odaklanır.
// NEDEN SHA-256?: Uzun LLM prompt'larını O(1) hızında aranabilir kısa bir Hash'e çeviriyoruz.

/// 1. Gelen soruyu (prompt) alır, benzersiz bir Redis anahtarı (Hash) üretir.
pub fn generate_cache_key(prompt: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prompt.as_bytes());
    let result = hasher.finalize();
    // Hash'i okunabilir bir hexadecimal string'e çeviriyoruz
    format!("cache:{:x}", result)
}

/// 2. Üretilen Hash'i Redis'te arar (Cache Hit / Miss).
pub async fn check_cache(
    redis_conn: &mut redis::aio::ConnectionManager,
    cache_key: &str,
) -> Option<String> {
    
    // Redis'ten veriyi çekmeyi deniyoruz
    match redis_conn.get::<_, String>(cache_key).await {
        Ok(cached_response) => {
            // ÖNLEME / OPTİMİZASYON: Eğer cevap Redis'te varsa, LLM faturası 0$ demektir.
            info!("🎯 [CACHE HIT] Semantic match found in Redis! Key: {}", cache_key);
            Some(cached_response)
        }
        Err(_) => {
            info!("⭕ [CACHE MISS] No match found. Prompt will be forwarded to Upstream LLM.");
            None
        }
    }
}
use reqwest::Client;
use serde_json::json;

// =====================================================================
// 🎯 VEKTÖR TEDARİKÇİSİ (Ollama Embedding İstemcisi)
// =====================================================================
pub async fn get_embedding(prompt: &str) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>> {
    // Çevresel değişkenden URL'i alıyoruz (Docker içindeyken host.docker.internal olacak)
    let embedding_url = std::env::var("EMBEDDING_URL")
        .unwrap_or_else(|_| "http://localhost:11434/api/embeddings".to_string());
    
    let client = Client::new();
    
    // Ollama'nın vektör modeline (nomic-embed-text) veriyi hazırlıyoruz
    let payload = json!({
        "model": "nomic-embed-text",
        "prompt": prompt
    });

    let response = client.post(&embedding_url)
        .json(&payload)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!("Ollama Embedding API Error: {}", response.status()).into());
    }

    let resp_json: serde_json::Value = response.json().await?;
    
    // Ollama'dan dönen JSON içindeki devasa Float dizisini (768 boyut), saf Rust f32 Vektörüne çeviriyoruz
    let embedding = resp_json["embedding"]
        .as_array()
        .ok_or("No embedding array found in response")?
        .iter()
        .filter_map(|v| v.as_f64().map(|f| f as f32))
        .collect::<Vec<f32>>();

    Ok(embedding)
}