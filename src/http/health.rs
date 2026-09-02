use actix_web::{get, HttpResponse, Responder};

#[get("/health/live")]
pub async fn liveness() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "alive"
    }))
}

#[get("/health/ready")]
pub async fn readiness() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ready"
    }))
}
