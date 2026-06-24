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