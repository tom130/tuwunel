use std::time::Duration;

use axum::{
	Json,
	extract::State,
	response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures::StreamExt;
use http::StatusCode;
use ruma::{DeviceId, OwnedDeviceId, OwnedUserId, UserId, api::client::error::ErrorKind};
use serde::Deserialize;
use serde_json::json;
use tuwunel_core::{Error, Result};
use tuwunel_service::{
	oauth_provider::grants::{GrantPoll, GrantStatus, PendingGrant},
	users::device::generate_refresh_token,
};

const DEVICE_CODE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:device_code";
const REFRESH_TOKEN_GRANT_TYPE: &str = "refresh_token";

#[derive(Debug, Deserialize)]
struct TokenRequest {
	grant_type: String,
	device_code: Option<String>,
	client_id: Option<String>,
	refresh_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DeviceTokenRequest {
	device_code: String,
	client_id: String,
}

#[derive(Debug, Deserialize)]
struct RefreshTokenRequest {
	grant_type: String,
	refresh_token: String,
}

#[derive(Debug, Eq, PartialEq)]
struct ApprovedDeviceGrant {
	user_id: OwnedUserId,
	device_id: OwnedDeviceId,
	scope: String,
}

#[derive(Debug, Eq, PartialEq)]
struct TokenIssue {
	access_token: String,
	refresh_token: String,
	expires_in: Duration,
	scope: String,
}

/// # `POST /_tuwunel/oauth/token`
///
/// Exchanges an approved OAuth device-code grant for native Matrix tokens.
pub(crate) async fn oauth_token_route(
	State(services): State<crate::State>,
	body: Bytes,
) -> Result<Response> {
	if !services.server.config.next_gen_auth {
		return Err(token_disabled_error());
	}

	let body = std::str::from_utf8(&body)?;
	let request: TokenRequest = serde_html_form::from_str(body)?;
	match request.grant_type.as_str() {
		| DEVICE_CODE_GRANT_TYPE => {
			let Some(device_code) = request.device_code else {
				return Ok(oauth_token_error_response(
					StatusCode::BAD_REQUEST,
					"invalid_request",
				));
			};
			let Some(client_id) = request.client_id else {
				return Ok(oauth_token_error_response(
					StatusCode::BAD_REQUEST,
					"invalid_request",
				));
			};

			device_code_token_grant(services, DeviceTokenRequest { device_code, client_id }).await
		},
		| REFRESH_TOKEN_GRANT_TYPE => {
			let Some(refresh_token) = request.refresh_token else {
				return Ok(oauth_token_error_response(
					StatusCode::BAD_REQUEST,
					"invalid_request",
				));
			};

			refresh_token_grant(services, RefreshTokenRequest {
				grant_type: request.grant_type,
				refresh_token,
			})
			.await
		},
		| _ => Ok(oauth_token_error_response(StatusCode::BAD_REQUEST, "unsupported_grant_type")),
	}
}

async fn device_code_token_grant(
	services: crate::State,
	request: DeviceTokenRequest,
) -> Result<Response> {
	match services
		.oauth_provider
		.clients
		.get(&request.client_id)
		.await
	{
		| Ok(_) => {},
		| Err(error) if error.is_not_found() => {
			return Ok(oauth_token_error_response(StatusCode::BAD_REQUEST, "invalid_client"));
		},
		| Err(error) => return Err(error),
	}

	let Ok(poll) = services
		.oauth_provider
		.grants
		.poll(&request.device_code)
		.await
	else {
		return Ok(oauth_token_error_response(StatusCode::BAD_REQUEST, "invalid_grant"));
	};

	let grant = match poll {
		| GrantPoll::Ready(grant) => *grant,
		| GrantPoll::SlowDown { .. } =>
			return Ok(oauth_token_error_response(StatusCode::BAD_REQUEST, "slow_down")),
		| GrantPoll::Expired =>
			return Ok(oauth_token_error_response(StatusCode::BAD_REQUEST, "expired_token")),
	};

	let approved = match approved_grant_for_issuance(&request, grant) {
		| Ok(approved) => approved,
		| Err(error) => return Ok(oauth_token_error_response(StatusCode::BAD_REQUEST, error)),
	};

	if device_id_owned_by_other_user(services, &approved.user_id, &approved.device_id).await {
		return Ok(oauth_token_error_response(StatusCode::BAD_REQUEST, "device_already_exists"));
	}

	let Ok(consumed) = services
		.oauth_provider
		.grants
		.consume(&request.device_code)
		.await
	else {
		return Ok(oauth_token_error_response(StatusCode::BAD_REQUEST, "invalid_grant"));
	};
	let approved = match approved_grant_for_issuance(&request, consumed) {
		| Ok(approved) => approved,
		| Err(error) => return Ok(oauth_token_error_response(StatusCode::BAD_REQUEST, error)),
	};

	let issue = issue_native_token(services, approved).await?;

	Ok(token_success_response(&issue))
}

async fn refresh_token_grant(
	services: crate::State,
	request: RefreshTokenRequest,
) -> Result<Response> {
	if request.grant_type != REFRESH_TOKEN_GRANT_TYPE
		|| !request.refresh_token.starts_with("refresh_")
	{
		return Ok(oauth_token_error_response(StatusCode::BAD_REQUEST, "invalid_grant"));
	}

	let Ok((user_id, device_id, ..)) = services
		.users
		.find_from_token(&request.refresh_token)
		.await
	else {
		return Ok(oauth_token_error_response(StatusCode::BAD_REQUEST, "invalid_grant"));
	};

	let refresh_token = generate_refresh_token();
	let (access_token, expires_in) = services.users.generate_access_token(true);
	let expires_in = expires_in.unwrap_or_default();

	services
		.users
		.set_access_token(
			&user_id,
			&device_id,
			&access_token,
			Some(expires_in),
			Some(&refresh_token),
		)
		.await?;

	let issue = TokenIssue {
		access_token,
		refresh_token,
		expires_in,
		scope: refresh_scope_for_device(device_id.as_str()),
	};

	Ok(token_success_response(&issue))
}

fn approved_grant_for_issuance(
	request: &DeviceTokenRequest,
	grant: PendingGrant,
) -> std::result::Result<ApprovedDeviceGrant, &'static str> {
	if request.client_id != grant.client_id {
		return Err("invalid_grant");
	}

	let GrantStatus::Approved { user_id } = grant.status else {
		return Err(match grant.status {
			| GrantStatus::Pending => "authorization_pending",
			| GrantStatus::Denied => "access_denied",
			| GrantStatus::Approved { .. } => unreachable!("approved handled above"),
		});
	};

	Ok(ApprovedDeviceGrant {
		user_id,
		device_id: OwnedDeviceId::from(grant.device_id),
		scope: grant.granted_scope.to_string(),
	})
}

async fn issue_native_token(
	services: crate::State,
	approved: ApprovedDeviceGrant,
) -> Result<TokenIssue> {
	let refresh_token = generate_refresh_token();
	let (access_token, expires_in) = services.users.generate_access_token(true);
	let expires_in = expires_in.unwrap_or_default();

	if services
		.users
		.device_exists(&approved.user_id, &approved.device_id)
		.await
	{
		services
			.users
			.set_access_token(
				&approved.user_id,
				&approved.device_id,
				&access_token,
				Some(expires_in),
				Some(&refresh_token),
			)
			.await?;
	} else {
		services
			.users
			.create_device(
				&approved.user_id,
				Some(&approved.device_id),
				(Some(&access_token), Some(expires_in)),
				Some(&refresh_token),
				Some("QR login"),
				None,
			)
			.await?;
	}

	Ok(TokenIssue {
		access_token,
		refresh_token,
		expires_in,
		scope: approved.scope,
	})
}

async fn device_id_owned_by_other_user(
	services: crate::State,
	user_id: &UserId,
	device_id: &DeviceId,
) -> bool {
	let users = services
		.users
		.stream()
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>()
		.await;

	for existing_user in users {
		if existing_user.as_str() != user_id.as_str()
			&& services
				.users
				.device_exists(&existing_user, device_id)
				.await
		{
			return true;
		}
	}

	false
}

fn token_success_response(issue: &TokenIssue) -> Response {
	(
		StatusCode::OK,
		Json(json!({
			"access_token": issue.access_token,
			"token_type": "Bearer",
			"expires_in": issue.expires_in.as_secs(),
			"refresh_token": issue.refresh_token,
			"scope": issue.scope,
		})),
	)
		.into_response()
}

fn refresh_scope_for_device(device_id: &str) -> String {
	format!("urn:matrix:client:device:{device_id} urn:matrix:client:api:*")
}

fn oauth_token_error_response(status: StatusCode, error: &str) -> Response {
	(status, Json(json!({ "error": error }))).into_response()
}

fn token_disabled_error() -> Error {
	Error::Request(ErrorKind::Unrecognized, "Unrecognized request.".into(), StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
	use std::{
		path::PathBuf,
		sync::Arc,
		time::{Duration, SystemTime, UNIX_EPOCH},
	};

	use axum::{
		Router,
		body::{Body, to_bytes},
		extract::State,
	};
	use http::{
		Request, StatusCode,
		header::{AUTHORIZATION, CONTENT_TYPE, ETAG, IF_MATCH},
	};
	use ruma::owned_user_id;
	use serde::Serialize;
	use serde_json::{Value as JsonValue, json};
	use tower::util::ServiceExt;
	use tracing::subscriber::NoSubscriber;
	use tuwunel_core::{
		config::{Config, Figment},
		log::{LogLevelReloadHandles, Logging, capture},
	};
	use tuwunel_service::{
		Services,
		oauth_provider::{
			clients::ClientRegistrationRequest,
			grants::{GrantStatus, PendingGrant},
			scope::parse_scope,
		},
	};

	use super::{
		ApprovedDeviceGrant, DEVICE_CODE_GRANT_TYPE, DeviceTokenRequest,
		REFRESH_TOKEN_GRANT_TYPE, RefreshTokenRequest, TokenIssue, approved_grant_for_issuance,
		oauth_token_error_response, oauth_token_route, refresh_scope_for_device,
		token_disabled_error, token_success_response,
	};
	use crate::client::oauth::revoke::oauth_revoke_route;

	#[tokio::test]
	async fn token_endpoint_rfc8628_errors_use_error_field() {
		let response =
			oauth_token_error_response(StatusCode::BAD_REQUEST, "authorization_pending");
		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::BAD_REQUEST);
		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		let json: JsonValue = serde_json::from_slice(&body).expect("body is json");
		assert_eq!(json["error"], "authorization_pending");
	}

	#[tokio::test]
	async fn token_endpoint_success_response_uses_bearer_seconds_and_scope() {
		let response = token_success_response(&TokenIssue {
			access_token: "access".into(),
			refresh_token: "refresh".into(),
			expires_in: Duration::from_secs(600),
			scope: "urn:matrix:client:device:DEVICE urn:matrix:client:api:*".into(),
		});
		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::OK);
		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		let json: JsonValue = serde_json::from_slice(&body).expect("body is json");
		assert_eq!(json["access_token"], "access");
		assert_eq!(json["refresh_token"], "refresh");
		assert_eq!(json["token_type"], "Bearer");
		assert_eq!(json["expires_in"], 600);
		assert_eq!(json["scope"], "urn:matrix:client:device:DEVICE urn:matrix:client:api:*",);
	}

	#[test]
	fn token_endpoint_approved_grant_binds_device_from_scope() {
		let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
		let request = DeviceTokenRequest {
			device_code: "device-code".into(),
			client_id: "client".into(),
		};
		let grant = PendingGrant {
			client_id: "client".into(),
			granted_scope: parse_scope(
				"urn:matrix:client:api:* urn:matrix:client:device:DEVICE_FROM_SCOPE",
			)
			.expect("scope parses"),
			device_id: "DEVICE_FROM_SCOPE".into(),
			user_code: "ABCD-EFGH".into(),
			created: now,
			expires: now + Duration::from_secs(600),
			interval: Duration::from_secs(5),
			last_polled: None,
			status: GrantStatus::Approved {
				user_id: owned_user_id!("@alice:example.com"),
			},
		};

		let ApprovedDeviceGrant { user_id, device_id, scope } =
			approved_grant_for_issuance(&request, grant).expect("grant is approved");

		assert_eq!(user_id, owned_user_id!("@alice:example.com"));
		assert_eq!(device_id.as_str(), "DEVICE_FROM_SCOPE");
		assert_eq!(scope, "urn:matrix:client:device:DEVICE_FROM_SCOPE urn:matrix:client:api:*",);
	}

	#[test]
	fn token_endpoint_ready_grant_states_map_to_rfc8628_errors() {
		let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
		let request = DeviceTokenRequest {
			device_code: "device-code".into(),
			client_id: "client".into(),
		};
		let grant = |status| PendingGrant {
			client_id: "client".into(),
			granted_scope: parse_scope("urn:matrix:client:api:* urn:matrix:client:device:DEVICE")
				.expect("scope parses"),
			device_id: "DEVICE".into(),
			user_code: "ABCD-EFGH".into(),
			created: now,
			expires: now + Duration::from_secs(600),
			interval: Duration::from_secs(5),
			last_polled: None,
			status,
		};

		assert_eq!(
			approved_grant_for_issuance(&request, grant(GrantStatus::Pending))
				.expect_err("pending grant is not issuable"),
			"authorization_pending",
		);
		assert_eq!(
			approved_grant_for_issuance(&request, grant(GrantStatus::Denied))
				.expect_err("denied grant is not issuable"),
			"access_denied",
		);

		let wrong_client = DeviceTokenRequest {
			client_id: "other-client".into(),
			..request
		};
		assert_eq!(
			approved_grant_for_issuance(
				&wrong_client,
				grant(GrantStatus::Approved {
					user_id: owned_user_id!("@alice:example.com"),
				}),
			)
			.expect_err("client mismatch is invalid"),
			"invalid_grant",
		);
	}

	#[test]
	fn oauth_refresh_scope_is_bound_to_existing_device_id() {
		let request = RefreshTokenRequest {
			grant_type: "refresh_token".into(),
			refresh_token: "refresh_token".into(),
		};

		assert_eq!(request.grant_type, "refresh_token");
		assert_eq!(
			refresh_scope_for_device("DEVICE"),
			"urn:matrix:client:device:DEVICE urn:matrix:client:api:*",
		);
	}

	#[test]
	fn token_endpoint_gate_off_error_is_unrecognized_404() {
		let error = token_disabled_error();
		assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
		assert_eq!(error.kind(), ruma::api::client::error::ErrorKind::Unrecognized,);
	}

	#[derive(Serialize)]
	struct DeviceGrantForm<'a> {
		grant_type: &'a str,
		device_code: &'a str,
		client_id: &'a str,
	}

	#[derive(Serialize)]
	struct RefreshGrantForm<'a> {
		grant_type: &'a str,
		refresh_token: &'a str,
	}

	#[derive(Serialize)]
	struct DeviceAuthorizationForm<'a> {
		client_id: &'a str,
		scope: &'a str,
	}

	#[derive(Serialize)]
	struct RevokeForm<'a> {
		token: &'a str,
	}

	#[tokio::test]
	async fn qr_login_router_conformance_sequence() -> tuwunel_core::Result {
		let (services, database_path) = test_services("qr-login-router-conformance").await?;
		let user_id = owned_user_id!("@qr-router:localhost");
		services
			.users
			.create(&user_id, Some("password"), None)
			.await?;

		let (state, guard) = crate::router::state::create(services.clone());
		let app = crate::router::build(Router::new(), &services.server).with_state(state);

		let (status, headers, created) = request_json(
			app.clone(),
			Request::builder()
				.method("POST")
				.uri("/_matrix/client/unstable/org.matrix.msc4108/rendezvous")
				.header(CONTENT_TYPE, "text/plain")
				.body(Body::from("m.login.protocols"))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::CREATED);
		let first_etag = headers
			.get(ETAG)
			.and_then(|value| value.to_str().ok())
			.expect("rendezvous create returns ETag")
			.to_owned();
		let session_url = created["url"]
			.as_str()
			.expect("rendezvous create returns session url");
		let session_path = session_url
			.strip_prefix("https://localhost")
			.expect("session url uses public base");

		let (status, headers, body) = request_bytes(
			app.clone(),
			Request::builder()
				.method("PUT")
				.uri(session_path)
				.header(CONTENT_TYPE, "text/plain")
				.header(IF_MATCH, first_etag.as_str())
				.body(Body::from("m.login.secrets"))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::ACCEPTED);
		assert!(body.is_empty());
		let second_etag = headers
			.get(ETAG)
			.and_then(|value| value.to_str().ok())
			.expect("rendezvous update returns ETag")
			.to_owned();
		assert_ne!(first_etag, second_etag);

		let (status, _, body) = request_bytes(
			app.clone(),
			Request::builder()
				.method("GET")
				.uri(session_path)
				.body(Body::empty())
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(body.as_ref(), b"m.login.secrets");

		let (status, _, metadata) = request_json(
			app.clone(),
			Request::builder()
				.method("GET")
				.uri("/_matrix/client/v1/auth_metadata")
				.body(Body::empty())
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(
			metadata["device_authorization_endpoint"],
			"https://localhost/_tuwunel/oauth/device",
		);

		let (status, _, registered) = request_json(
			app.clone(),
			Request::builder()
				.method("POST")
				.uri("/_tuwunel/oauth/register")
				.header(CONTENT_TYPE, "application/json")
				.body(Body::from(
					json!({
						"client_name": "QR router test client",
						"client_uri": "https://client.example/",
						"grant_types": [DEVICE_CODE_GRANT_TYPE, REFRESH_TOKEN_GRANT_TYPE],
						"token_endpoint_auth_method": "none",
					})
					.to_string(),
				))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::CREATED);
		let client_id = registered["client_id"]
			.as_str()
			.expect("registered client id returned");

		let scope = "urn:matrix:client:api:* urn:matrix:client:device:DEVICEQRHTTP";
		let device_body =
			serde_html_form::to_string(&DeviceAuthorizationForm { client_id, scope })?;
		let (status, _, device) = request_json(
			app.clone(),
			Request::builder()
				.method("POST")
				.uri("/_tuwunel/oauth/device")
				.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
				.body(Body::from(device_body))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(device["interval"], 5);
		let device_code = device["device_code"]
			.as_str()
			.expect("device code returned");
		let user_code = device["user_code"]
			.as_str()
			.expect("user code returned");

		services
			.oauth_provider
			.grants
			.approve(user_code, user_id.clone())
			.await?;

		let token_body = serde_html_form::to_string(&DeviceGrantForm {
			grant_type: DEVICE_CODE_GRANT_TYPE,
			device_code,
			client_id,
		})?;
		let (status, _, token) = request_json(
			app.clone(),
			Request::builder()
				.method("POST")
				.uri("/_tuwunel/oauth/token")
				.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
				.body(Body::from(token_body))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(token["token_type"], "Bearer");
		let access_token = token["access_token"]
			.as_str()
			.expect("access token returned");
		let (found_user, found_device, _) = services
			.users
			.find_from_token(access_token)
			.await?;
		assert_eq!(found_user, user_id);
		assert_eq!(found_device.as_str(), "DEVICEQRHTTP");

		let (status, _, device) = request_json(
			app.clone(),
			Request::builder()
				.method("GET")
				.uri("/_matrix/client/v3/devices/DEVICEQRHTTP")
				.header(AUTHORIZATION, format!("Bearer {access_token}"))
				.body(Body::empty())
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(device["device_id"], "DEVICEQRHTTP");

		drop(app);
		drop(guard);
		drop(services);
		std::fs::remove_dir_all(database_path).ok();

		Ok(())
	}

	#[tokio::test]
	async fn token_endpoint_oauth_refresh_oauth_revoke_conformance() -> tuwunel_core::Result {
		let (services, database_path) = test_services("qr-login-token-conformance").await?;
		let (state, guard) = crate::router::state::create(services.clone());
		let user_id = owned_user_id!("@qr:localhost");
		services
			.users
			.create(&user_id, Some("password"), None)
			.await?;
		let registered = services
			.oauth_provider
			.clients
			.register(ClientRegistrationRequest {
				client_name: Some("QR test client".into()),
				client_uri: Some("https://client.example/".into()),
				grant_types: vec![DEVICE_CODE_GRANT_TYPE.into(), REFRESH_TOKEN_GRANT_TYPE.into()],
				..Default::default()
			})
			.await?;
		let created = services
			.oauth_provider
			.grants
			.create(
				registered.client_id.clone(),
				parse_scope("urn:matrix:client:api:* urn:matrix:client:device:DEVICEQR")?,
			)
			.await?;
		services
			.oauth_provider
			.grants
			.approve(&created.user_code, user_id.clone())
			.await?;

		let first = post_token(state, &DeviceGrantForm {
			grant_type: DEVICE_CODE_GRANT_TYPE,
			device_code: &created.device_code,
			client_id: &registered.client_id,
		})
		.await;
		assert_eq!(first["token_type"], "Bearer");
		let access_token = first["access_token"]
			.as_str()
			.expect("access token returned")
			.to_owned();
		let refresh_token = first["refresh_token"]
			.as_str()
			.expect("refresh token returned")
			.to_owned();
		let (found_user, found_device, _) = services
			.users
			.find_from_token(&access_token)
			.await?;
		assert_eq!(found_user, user_id);
		assert_eq!(found_device.as_str(), "DEVICEQR");

		let second = post_token(state, &DeviceGrantForm {
			grant_type: DEVICE_CODE_GRANT_TYPE,
			device_code: &created.device_code,
			client_id: &registered.client_id,
		})
		.await;
		assert_eq!(second["error"], "invalid_grant");

		let refreshed = post_token(state, &RefreshGrantForm {
			grant_type: REFRESH_TOKEN_GRANT_TYPE,
			refresh_token: &refresh_token,
		})
		.await;
		assert!(
			services
				.users
				.find_from_token(&access_token)
				.await
				.is_err(),
			"refresh invalidates the old access token",
		);
		let refreshed_access = refreshed["access_token"]
			.as_str()
			.expect("refreshed access token returned")
			.to_owned();
		let (found_user, found_device, _) = services
			.users
			.find_from_token(&refreshed_access)
			.await?;
		assert_eq!(found_user, user_id);
		assert_eq!(found_device.as_str(), "DEVICEQR");

		let revoke_body = serde_html_form::to_string(&RevokeForm { token: &refreshed_access })?;
		let revoke = oauth_revoke_route(State(state), revoke_body.into()).await?;
		assert_eq!(revoke.status(), StatusCode::OK);
		assert!(
			services
				.users
				.find_from_token(&refreshed_access)
				.await
				.is_err(),
			"revoke removes the refreshed access token",
		);

		drop(guard);
		drop(services);
		std::fs::remove_dir_all(database_path).ok();

		Ok(())
	}

	async fn post_token<Form>(state: crate::State, form: &Form) -> JsonValue
	where
		Form: Serialize + Sync,
	{
		let body = serde_html_form::to_string(form).expect("form serializes");
		let response = oauth_token_route(State(state), body.into())
			.await
			.expect("token route succeeds");
		let (parts, body) = response.into_parts();
		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		let json: JsonValue = serde_json::from_slice(&body).expect("body is json");
		if parts.status != StatusCode::OK {
			return json;
		}

		assert!(json.get("access_token").is_some());
		json
	}

	async fn request_json(
		app: Router,
		request: Request<Body>,
	) -> (StatusCode, http::HeaderMap, JsonValue) {
		let (status, headers, body) = request_bytes(app, request).await;
		let json = serde_json::from_slice(&body).expect("response body is json");

		(status, headers, json)
	}

	async fn request_bytes(
		app: Router,
		request: Request<Body>,
	) -> (StatusCode, http::HeaderMap, bytes::Bytes) {
		let response = app
			.oneshot(request)
			.await
			.expect("router handles request");
		let (parts, body) = response.into_parts();
		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");

		(parts.status, parts.headers, body)
	}

	async fn test_services(name: &str) -> tuwunel_core::Result<(Arc<Services>, PathBuf)> {
		let path = std::env::temp_dir().join(format!(
			"tuwunel-{name}-{}-{}",
			std::process::id(),
			SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("system time after epoch")
				.as_nanos(),
		));
		let raw = Figment::new()
			.merge(("server_name", "localhost"))
			.merge(("database_path", path.to_string_lossy().to_string()))
			.merge(("next_gen_auth", true))
			.merge(("create_admin_room", false))
			.merge(("log_enable", false))
			.merge(("test", vec!["fresh".to_owned(), "cleanup".to_owned()]));
		let config = Config::new(&raw)?;
		let server = Arc::new(tuwunel_core::Server::new(
			config,
			Some(&tokio::runtime::Handle::current()),
			Logging {
				reload: LogLevelReloadHandles::default(),
				capture: Arc::new(capture::State::new()),
				subscriber: Arc::new(NoSubscriber::new()),
			},
		));

		Services::build(server)
			.await
			.map(|services| (services, path))
	}
}
