# ==========================================
# FAZ 1: İNŞAAT ALANI (Builder)
# ==========================================
FROM rust:slim AS builder

# 🎯 YENİ: reqwest ve şifreleme modülleri için eksik olan alet çantasını (OpenSSL) şantiyeye indiriyoruz!
RUN apt-get update && apt-get install -y pkg-config libssl-dev

# Şantiyemizi kuruyoruz
WORKDIR /usr/src/kalkan-ai

# Önbelleği (Cache) akıllı kullanmak için önce sadece bağımlılık listesini kopyalıyoruz
COPY Cargo.toml ./
COPY src ./src

# Motoru 'Release' (Canlı Ortam - Maksimum Performans) modunda derle
RUN cargo build --release

# ==========================================
# FAZ 2: CANLI ORTAM ZIRHI (Runtime)
# ==========================================
# Sadece 30-40 MB boyutunda tertemiz bir Linux tabanı alıyoruz
FROM debian:bookworm-slim

# Dış ağlara güvenli HTTPS/SSL bağlantısı yapabilmek için gerekli sertifikaları kuruyoruz
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Faz 1'deki devasa inşaat alanından sadece "derlenmiş saf motoru" çekip alıyoruz
COPY --from=builder /usr/src/kalkan-ai/target/release/kalkan-ai /app/kalkan-ai

# KalkanAI'nin dış dünya ile konuşacağı kapıyı tanımlıyoruz
EXPOSE 8080

# Çevresel Değişkenler (Dışarıdan yönetilebilir esneklik)
ENV RUST_LOG="info"
ENV UPSTREAM_URL="http://host.docker.internal:11434/v1/chat/completions"

# Motoru ateşle!
CMD ["./kalkan-ai"]