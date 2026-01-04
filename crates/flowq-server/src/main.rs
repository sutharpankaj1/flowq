//! FlowQ Server - Message Broker HTTP Server
//!
//! This is the main entry point for the FlowQ message broker.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use flowq_core::Broker;
use flowq_storage::MemoryStorage;
use flowq_types::{Error, Message, Queue, QueueConfig, QueueStats};
use serde::{Deserialize, Serialize};
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

// ==================== App State ====================

/// Shared application state
#[derive(Clone)]
struct AppState {
    broker: Arc<Broker>,
}

// ==================== Request/Response Types ====================

/// Create queue request
#[derive(Debug, Deserialize, ToSchema)]
struct CreateQueueRequest {
    /// Name of the queue to create
    name: String,
    /// Optional queue configuration
    #[serde(default)]
    config: Option<QueueConfig>,
}

/// Publish message request
#[derive(Debug, Deserialize, ToSchema)]
struct PublishRequest {
    /// Message body content
    body: String,
    /// Content type (e.g., "application/json")
    #[serde(default)]
    content_type: Option<String>,
    /// Message priority (1-10, higher = more important)
    #[serde(default)]
    priority: Option<u8>,
    /// Custom message attributes
    #[serde(default)]
    attributes: Option<std::collections::HashMap<String, String>>,
}

/// Publish response
#[derive(Debug, Serialize, ToSchema)]
struct PublishResponse {
    /// ID of the published message
    message_id: String,
}

/// Receive query parameters
#[derive(Debug, Deserialize, ToSchema)]
struct ReceiveQuery {
    /// Maximum number of messages to receive (default: 1)
    #[serde(default = "default_max_messages")]
    max: usize,
}

fn default_max_messages() -> usize {
    1
}

/// Message response (for API)
#[derive(Debug, Serialize, ToSchema)]
struct MessageResponse {
    /// Unique message ID
    id: String,
    /// Message body content
    body: String,
    /// Content type
    content_type: Option<String>,
    /// Message priority
    priority: u8,
    /// Number of delivery attempts
    delivery_count: u32,
    /// Custom attributes
    attributes: std::collections::HashMap<String, String>,
    /// Creation timestamp
    created_at: String,
}

impl From<Message> for MessageResponse {
    fn from(msg: Message) -> Self {
        Self {
            id: msg.id.to_string(),
            body: msg.body_as_str().unwrap_or("").to_string(),
            content_type: msg.content_type,
            priority: msg.priority,
            delivery_count: msg.delivery_count,
            attributes: msg.attributes,
            created_at: msg.created_at.to_rfc3339(),
        }
    }
}

/// Ack/Nack request
#[derive(Debug, Deserialize, ToSchema)]
struct AckRequest {
    /// ID of the message to acknowledge
    message_id: String,
}

/// API Error response
#[derive(Debug, Serialize, ToSchema)]
struct ApiErrorBody {
    /// Error message
    error: String,
    /// Error code
    code: String,
}

/// Purge response
#[derive(Debug, Serialize, ToSchema)]
struct PurgeResponse {
    /// Number of messages purged
    purged: u64,
}

/// Health check response
#[derive(Debug, Serialize, ToSchema)]
struct HealthResponse {
    /// Health status
    status: String,
    /// Server version
    version: String,
}

// ==================== Error Handling ====================

/// Wrapper for FlowQ errors to implement IntoResponse
struct AppError(Error);

impl From<Error> for AppError {
    fn from(err: Error) -> Self {
        AppError(err)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, code) = match &self.0 {
            Error::QueueNotFound(_) => (StatusCode::NOT_FOUND, "QUEUE_NOT_FOUND"),
            Error::QueueAlreadyExists(_) => (StatusCode::CONFLICT, "QUEUE_ALREADY_EXISTS"),
            Error::MessageNotFound(_) => (StatusCode::NOT_FOUND, "MESSAGE_NOT_FOUND"),
            Error::QueueFull(_) => (StatusCode::SERVICE_UNAVAILABLE, "QUEUE_FULL"),
            Error::QueueEmpty(_) => (StatusCode::NO_CONTENT, "QUEUE_EMPTY"),
            Error::InvalidMessage(_) => (StatusCode::BAD_REQUEST, "INVALID_MESSAGE"),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
        };

        let body = Json(ApiErrorBody {
            error: self.0.to_string(),
            code: code.to_string(),
        });

        (status, body).into_response()
    }
}

// ==================== OpenAPI Documentation ====================

#[derive(OpenApi)]
#[openapi(
    info(
        title = "FlowQ API",
        version = "0.1.0",
        description = "FlowQ - Open Source Message Broker API",
        license(name = "MIT OR Apache-2.0"),
        contact(name = "FlowQ Team", url = "https://github.com/flowq/flowq")
    ),
    servers(
        (url = "http://localhost:3000", description = "Local development server")
    ),
    paths(
        health,
        list_queues,
        create_queue,
        get_queue,
        delete_queue,
        get_queue_stats,
        purge_queue,
        publish_message,
        receive_messages,
        ack_message,
        nack_message,
    ),
    components(
        schemas(
            HealthResponse,
            Queue,
            QueueConfig,
            QueueStats,
            CreateQueueRequest,
            PublishRequest,
            PublishResponse,
            MessageResponse,
            ReceiveQuery,
            AckRequest,
            ApiErrorBody,
            PurgeResponse,
        )
    ),
    tags(
        (name = "health", description = "Health check endpoints"),
        (name = "queues", description = "Queue management endpoints"),
        (name = "messages", description = "Message operations endpoints")
    )
)]
struct ApiDoc;

// ==================== Handlers ====================

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses(
        (status = 200, description = "Server is healthy", body = HealthResponse)
    )
)]
async fn health() -> impl IntoResponse {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// List all queues
#[utoipa::path(
    get,
    path = "/api/v1/queues",
    tag = "queues",
    responses(
        (status = 200, description = "List of all queues", body = Vec<Queue>)
    )
)]
async fn list_queues(State(state): State<AppState>) -> Result<Json<Vec<Queue>>, AppError> {
    let queues = state.broker.list_queues().await?;
    Ok(Json(queues))
}

/// Create a new queue
#[utoipa::path(
    post,
    path = "/api/v1/queues",
    tag = "queues",
    request_body = CreateQueueRequest,
    responses(
        (status = 201, description = "Queue created successfully", body = Queue),
        (status = 409, description = "Queue already exists", body = ApiErrorBody)
    )
)]
async fn create_queue(
    State(state): State<AppState>,
    Json(req): Json<CreateQueueRequest>,
) -> Result<(StatusCode, Json<Queue>), AppError> {
    let queue = match req.config {
        Some(config) => {
            state
                .broker
                .create_queue_with_config(req.name, config)
                .await?
        }
        None => state.broker.create_queue(req.name).await?,
    };

    Ok((StatusCode::CREATED, Json(queue)))
}

/// Get queue details
#[utoipa::path(
    get,
    path = "/api/v1/queues/{name}",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    responses(
        (status = 200, description = "Queue details", body = Queue),
        (status = 404, description = "Queue not found", body = ApiErrorBody)
    )
)]
async fn get_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Queue>, AppError> {
    let queue = state
        .broker
        .get_queue(&name)
        .await?
        .ok_or_else(|| Error::QueueNotFound(name))?;

    Ok(Json(queue))
}

/// Delete a queue
#[utoipa::path(
    delete,
    path = "/api/v1/queues/{name}",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    responses(
        (status = 204, description = "Queue deleted successfully"),
        (status = 404, description = "Queue not found", body = ApiErrorBody)
    )
)]
async fn delete_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, AppError> {
    state.broker.delete_queue(&name).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Get queue statistics
#[utoipa::path(
    get,
    path = "/api/v1/queues/{name}/stats",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    responses(
        (status = 200, description = "Queue statistics", body = QueueStats),
        (status = 404, description = "Queue not found", body = ApiErrorBody)
    )
)]
async fn get_queue_stats(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<QueueStats>, AppError> {
    let stats = state.broker.get_queue_stats(&name).await?;
    Ok(Json(stats))
}

/// Purge all messages from a queue
#[utoipa::path(
    post,
    path = "/api/v1/queues/{name}/purge",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    responses(
        (status = 200, description = "Queue purged", body = PurgeResponse),
        (status = 404, description = "Queue not found", body = ApiErrorBody)
    )
)]
async fn purge_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<PurgeResponse>, AppError> {
    let count = state.broker.purge_queue(&name).await?;
    Ok(Json(PurgeResponse { purged: count }))
}

/// Publish a message to a queue
#[utoipa::path(
    post,
    path = "/api/v1/queues/{name}/messages",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    request_body = PublishRequest,
    responses(
        (status = 201, description = "Message published", body = PublishResponse),
        (status = 404, description = "Queue not found", body = ApiErrorBody)
    )
)]
async fn publish_message(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<PublishRequest>,
) -> Result<(StatusCode, Json<PublishResponse>), AppError> {
    let mut message = Message::new(req.body);

    if let Some(ct) = req.content_type {
        message = message.with_content_type(ct);
    }

    if let Some(p) = req.priority {
        message = message.with_priority(p);
    }

    if let Some(attrs) = req.attributes {
        for (k, v) in attrs {
            message = message.with_attribute(k, v);
        }
    }

    let message_id = state.broker.publish(&queue_name, message).await?;

    Ok((
        StatusCode::CREATED,
        Json(PublishResponse {
            message_id: message_id.to_string(),
        }),
    ))
}

/// Receive messages from a queue
#[utoipa::path(
    get,
    path = "/api/v1/queues/{name}/messages",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name"),
        ("max" = Option<usize>, Query, description = "Maximum messages to receive")
    ),
    responses(
        (status = 200, description = "Messages received", body = Vec<MessageResponse>),
        (status = 404, description = "Queue not found", body = ApiErrorBody)
    )
)]
async fn receive_messages(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Query(query): Query<ReceiveQuery>,
) -> Result<Json<Vec<MessageResponse>>, AppError> {
    let messages = state.broker.receive_batch(&queue_name, query.max).await?;
    let responses: Vec<MessageResponse> = messages.into_iter().map(Into::into).collect();
    Ok(Json(responses))
}

/// Acknowledge a message
#[utoipa::path(
    post,
    path = "/api/v1/queues/{name}/messages/ack",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    request_body = AckRequest,
    responses(
        (status = 204, description = "Message acknowledged"),
        (status = 404, description = "Message not found", body = ApiErrorBody)
    )
)]
async fn ack_message(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<AckRequest>,
) -> Result<StatusCode, AppError> {
    let message_id = flowq_types::MessageId(
        req.message_id
            .parse()
            .map_err(|_| Error::InvalidMessage("Invalid message ID".to_string()))?,
    );

    state.broker.ack(&queue_name, &message_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Negative acknowledge a message (return to queue)
#[utoipa::path(
    post,
    path = "/api/v1/queues/{name}/messages/nack",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name")
    ),
    request_body = AckRequest,
    responses(
        (status = 204, description = "Message returned to queue"),
        (status = 404, description = "Message not found", body = ApiErrorBody)
    )
)]
async fn nack_message(
    State(state): State<AppState>,
    Path(queue_name): Path<String>,
    Json(req): Json<AckRequest>,
) -> Result<StatusCode, AppError> {
    let message_id = flowq_types::MessageId(
        req.message_id
            .parse()
            .map_err(|_| Error::InvalidMessage("Invalid message ID".to_string()))?,
    );

    state.broker.nack(&queue_name, &message_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

// ==================== Router ====================

fn create_router(state: AppState) -> Router {
    Router::new()
        // Swagger UI
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        // Health
        .route("/health", get(health))
        // Queues
        .route("/api/v1/queues", get(list_queues).post(create_queue))
        .route(
            "/api/v1/queues/:name",
            get(get_queue).delete(delete_queue),
        )
        .route("/api/v1/queues/:name/stats", get(get_queue_stats))
        .route("/api/v1/queues/:name/purge", post(purge_queue))
        // Messages
        .route(
            "/api/v1/queues/:name/messages",
            post(publish_message).get(receive_messages),
        )
        .route("/api/v1/queues/:name/messages/ack", post(ack_message))
        .route("/api/v1/queues/:name/messages/nack", post(nack_message))
        // Middleware
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

// ==================== Main ====================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "flowq=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Create broker with in-memory storage
    let storage = MemoryStorage::new();
    let broker = Arc::new(Broker::new(storage));

    // Start maintenance tasks
    broker.start_maintenance().await;

    // Create app state
    let state = AppState { broker };

    // Create router
    let app = create_router(state);

    // Start server
    let addr = "127.0.0.1:3000";
    let listener = tokio::net::TcpListener::bind(addr).await?;

    info!("FlowQ server listening on {}", addr);
    info!("Swagger UI: http://localhost:3000/swagger-ui/");
    info!("Health check: http://localhost:3000/health");

    axum::serve(listener, app).await?;

    Ok(())
}
