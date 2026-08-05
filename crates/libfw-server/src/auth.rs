//! Bearer-token extraction and authorization rejections.

use std::sync::Arc;

use axum::extract::{FromRef, FromRequestParts};
use axum::http::request::Parts;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use libfw_core::auth::AuthError;
use libfw_core::claims::TokenClaims;

use crate::ServerState;

/// A request rejection caused by missing/invalid credentials.
#[derive(Debug, thiserror::Error)]
pub enum AuthRejection {
    /// `401 Unauthorized` — missing or unverifiable token.
    #[error("{0}")]
    Unauthorized(String),
    /// `403 Forbidden` — valid token but insufficient rights.
    #[error("permission denied: {action} on `{path}`")]
    Forbidden { path: String, action: String },
}

impl From<AuthError> for AuthRejection {
    fn from(err: AuthError) -> Self {
        match err {
            AuthError::MissingToken => AuthRejection::Unauthorized("missing bearer token".into()),
            AuthError::Invalid(msg) => AuthRejection::Unauthorized(format!("invalid token: {msg}")),
            AuthError::Expired => AuthRejection::Unauthorized("token expired".into()),
            AuthError::Forbidden { path, action } => {
                AuthRejection::Forbidden { path, action: action.to_string() }
            }
        }
    }
}

impl IntoResponse for AuthRejection {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AuthRejection::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg),
            AuthRejection::Forbidden { path, action } => (
                StatusCode::FORBIDDEN,
                format!("permission denied: {action} on `{path}`"),
            ),
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

/// Extracts and verifies the `Authorization: Bearer <token>` header.
///
/// Verification is delegated to [`ServerState::verifier`]; path-level
/// authorization happens in the handler via
/// [`ServerState::authorize`].
pub struct BearerClaims(pub TokenClaims);

impl<S> FromRequestParts<S> for BearerClaims
where
    S: Send + Sync,
    Arc<ServerState>: axum::extract::FromRef<S>,
{
    type Rejection = AuthRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let state = Arc::<ServerState>::from_ref(state);
        let header_value = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| AuthRejection::Unauthorized("missing bearer token".into()))?;
        let token = header_value
            .strip_prefix("Bearer ")
            .ok_or_else(|| AuthRejection::Unauthorized("malformed authorization header".into()))?
            .trim();
        let claims = state
            .verifier
            .verify(token)
            .map_err(AuthRejection::from)?;
        Ok(BearerClaims(claims))
    }
}
