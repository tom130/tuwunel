use axum::{
	extract::State,
	response::{IntoResponse, Response},
};
use axum_extra::extract::cookie::CookieJar;
use bytes::Bytes;
use http::{HeaderValue, StatusCode, Uri, header::LOCATION};
use ruma::{OwnedUserId, api::client::error::ErrorKind};
use serde::Deserialize;
use tuwunel_core::{Err, Error, Result, err};
use tuwunel_service::oauth_provider::{
	authorize::PendingAuthorization, clients::RegisteredClient, consent::BrowserSession,
	pkce::S256, redirect::redirect_uri_matches, scope::parse_scope,
};
use url::Url;

use super::web::{
	append_consent_cookie, consent_session_from_cookie, escape_html, html_response,
	message_response, public_base_url, sso_provider_links,
};

const LOGIN_TEMPLATE: &str = include_str!("templates/authorize_login.html");
const CONSENT_TEMPLATE: &str = include_str!("templates/authorize_consent.html");

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum AuthorizeAction {
	Login,
	Approve,
	Deny,
}

#[derive(Debug, Deserialize)]
struct AuthorizePostForm {
	action: AuthorizeAction,
	auth_id: String,
	csrf: Option<String>,
	username: Option<String>,
	password: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
struct AuthorizeDecision {
	action: AuthorizeAction,
	auth_id: String,
	user_id: OwnedUserId,
}

#[derive(Debug, Deserialize)]
struct AuthorizeQuery {
	response_type: Option<String>,
	client_id: Option<String>,
	redirect_uri: Option<String>,
	scope: Option<String>,
	state: Option<String>,
	code_challenge: Option<String>,
	code_challenge_method: Option<String>,
	auth_id: Option<String>,
	#[serde(rename = "loginToken")]
	login_token: Option<String>,
	#[allow(dead_code)]
	prompt: Option<String>,
	#[allow(dead_code)]
	login_hint: Option<String>,
}

/// # `GET /_tuwunel/oauth/authorize`
///
/// Browser-facing OAuth 2.0 authorization-code endpoint.
pub(crate) async fn get_oauth_authorize_route(
	State(services): State<crate::State>,
	jar: CookieJar,
	uri: Uri,
) -> Result<Response> {
	if !services.server.config.next_gen_auth {
		return Err(authorize_disabled_error());
	}

	let query: AuthorizeQuery = serde_html_form::from_str(uri.query().unwrap_or_default())?;
	if let Some(auth_id) = query
		.auth_id
		.as_deref()
		.map(str::trim)
		.filter(|auth_id| !auth_id.is_empty())
	{
		return resumed_authorization_response(
			services,
			jar,
			auth_id,
			query.login_token.as_deref(),
		)
		.await;
	}

	let Some(client_id) = query
		.client_id
		.as_deref()
		.map(str::trim)
		.filter(|client_id| !client_id.is_empty())
	else {
		return Ok(authorization_request_error_response(StatusCode::BAD_REQUEST));
	};
	let client = match services
		.oauth_provider
		.clients
		.get(client_id)
		.await
	{
		| Ok(client) => client,
		| Err(error) if error.is_not_found() => {
			return Ok(authorization_request_error_response(StatusCode::BAD_REQUEST));
		},
		| Err(error) => return Err(error),
	};

	let Some(redirect_uri) = query
		.redirect_uri
		.as_deref()
		.map(str::trim)
		.filter(|redirect_uri| !redirect_uri.is_empty())
	else {
		return Ok(authorization_request_error_response(StatusCode::BAD_REQUEST));
	};
	if !client
		.metadata
		.redirect_uris
		.iter()
		.any(|registered| redirect_uri_matches(registered, redirect_uri))
	{
		return Ok(authorization_request_error_response(StatusCode::BAD_REQUEST));
	}

	let state = query.state.unwrap_or_default();
	if query.response_type.as_deref() != Some("code") {
		return authorization_error_redirect(redirect_uri, "unsupported_response_type", &state);
	}
	let Some(code_challenge) = query
		.code_challenge
		.as_deref()
		.map(str::trim)
		.filter(|challenge| !challenge.is_empty())
	else {
		return authorization_error_redirect(redirect_uri, "invalid_request", &state);
	};
	if query.code_challenge_method.as_deref() != Some(S256) {
		return authorization_error_redirect(redirect_uri, "invalid_request", &state);
	}
	let Some(scope) = query.scope.as_deref() else {
		return authorization_error_redirect(redirect_uri, "invalid_scope", &state);
	};
	let Ok(granted_scope) = parse_scope(scope) else {
		return authorization_error_redirect(redirect_uri, "invalid_scope", &state);
	};

	let created = services
		.oauth_provider
		.authorize
		.pending
		.create(client_id, redirect_uri, granted_scope, code_challenge, state)
		.await?;

	let created_session = if let Some(login_token) = query.login_token.as_deref() {
		let user_id = services
			.users
			.find_from_login_token(login_token)
			.await?;
		Some(
			services
				.oauth_provider
				.consent
				.create(user_id)
				.await,
		)
	} else {
		None
	};
	let session = match created_session.as_ref() {
		| Some(created) => Some(created.session.clone()),
		| None => consent_session_from_cookie(&services, &jar).await,
	};

	let base_url = public_base_url(services);
	let sso_links = sso_provider_links(services, &base_url, "/_tuwunel/oauth/authorize", &[(
		"auth_id",
		created.auth_id.as_str(),
	)])?;
	let mut response = authorize_page_response(
		&created.authorization,
		&created.auth_id,
		Some(&client),
		session.as_ref(),
		&sso_links,
	);

	if let Some(created) = created_session.as_ref() {
		append_consent_cookie(&mut response, created, base_url.starts_with("https://"))?;
	}

	Ok(response)
}

/// # `POST /_tuwunel/oauth/authorize`
///
/// Handles login, approval, and denial form submissions for auth-code login.
pub(crate) async fn post_oauth_authorize_route(
	State(services): State<crate::State>,
	jar: CookieJar,
	body: Bytes,
) -> Result<Response> {
	if !services.server.config.next_gen_auth {
		return Err(authorize_disabled_error());
	}

	let body = std::str::from_utf8(&body)?;
	let form: AuthorizePostForm = serde_html_form::from_str(body)?;
	let Ok(pending) = services
		.oauth_provider
		.authorize
		.pending
		.get(&form.auth_id)
		.await
	else {
		return Ok(authorization_request_error_response(StatusCode::NOT_FOUND));
	};

	match form.action {
		| AuthorizeAction::Login => password_login_response(&services, &form, &pending).await,
		| AuthorizeAction::Approve => {
			let session = consent_session_from_cookie(&services, &jar).await;
			let Ok(decision) = validated_authorize_decision(&form, session.as_ref()) else {
				return Ok(message_response(
					StatusCode::FORBIDDEN,
					"Approval session expired",
					"Open the OAuth login link again and sign in before approving.",
				));
			};

			let Ok(pending) = services
				.oauth_provider
				.authorize
				.pending
				.consume(&decision.auth_id)
				.await
			else {
				return Ok(authorization_request_error_response(StatusCode::NOT_FOUND));
			};
			let created = services
				.oauth_provider
				.authorize
				.codes
				.create(
					pending.client_id,
					pending.redirect_uri.clone(),
					pending.granted_scope,
					decision.user_id,
					pending.code_challenge,
				)
				.await?;

			authorization_success_redirect(&pending.redirect_uri, &created.code, &pending.state)
		},
		| AuthorizeAction::Deny => {
			let Ok(pending) = services
				.oauth_provider
				.authorize
				.pending
				.consume(&form.auth_id)
				.await
			else {
				return Ok(authorization_request_error_response(StatusCode::NOT_FOUND));
			};

			authorization_error_redirect(&pending.redirect_uri, "access_denied", &pending.state)
		},
	}
}

fn validated_authorize_decision(
	form: &AuthorizePostForm,
	session: Option<&BrowserSession>,
) -> Result<AuthorizeDecision> {
	let session = session.ok_or_else(|| err!(Request(Forbidden("Consent session required."))))?;
	let csrf = form
		.csrf
		.as_deref()
		.ok_or_else(|| err!(Request(Forbidden("CSRF token required."))))?;

	if csrf != session.csrf_token {
		return Err!(Request(Forbidden("Invalid CSRF token.")));
	}

	Ok(AuthorizeDecision {
		action: form.action,
		auth_id: form.auth_id.clone(),
		user_id: session.user_id.clone(),
	})
}

async fn resumed_authorization_response(
	services: crate::State,
	jar: CookieJar,
	auth_id: &str,
	login_token: Option<&str>,
) -> Result<Response> {
	let Ok(pending) = services
		.oauth_provider
		.authorize
		.pending
		.get(auth_id)
		.await
	else {
		return Ok(authorization_request_error_response(StatusCode::NOT_FOUND));
	};
	let client = services
		.oauth_provider
		.clients
		.get(&pending.client_id)
		.await
		.ok();

	let created_session = if let Some(login_token) = login_token {
		let user_id = services
			.users
			.find_from_login_token(login_token)
			.await?;
		Some(
			services
				.oauth_provider
				.consent
				.create(user_id)
				.await,
		)
	} else {
		None
	};
	let session = match created_session.as_ref() {
		| Some(created) => Some(created.session.clone()),
		| None => consent_session_from_cookie(&services, &jar).await,
	};

	let base_url = public_base_url(services);
	let sso_links = sso_provider_links(services, &base_url, "/_tuwunel/oauth/authorize", &[(
		"auth_id", auth_id,
	)])?;
	let mut response =
		authorize_page_response(&pending, auth_id, client.as_ref(), session.as_ref(), &sso_links);

	if let Some(created) = created_session.as_ref() {
		append_consent_cookie(&mut response, created, base_url.starts_with("https://"))?;
	}

	Ok(response)
}

async fn password_login_response(
	services: &crate::State,
	form: &AuthorizePostForm,
	pending: &PendingAuthorization,
) -> Result<Response> {
	let Some(username) = form
		.username
		.as_deref()
		.map(str::trim)
		.filter(|username| !username.is_empty())
	else {
		return authorize_login_error_response(
			*services,
			pending,
			&form.auth_id,
			"Username is required.",
		)
		.await;
	};
	let Some(password) = form.password.as_deref() else {
		return authorize_login_error_response(
			*services,
			pending,
			&form.auth_id,
			"Password is required.",
		)
		.await;
	};

	let Ok(user_id) =
		crate::client::session::authenticate_password_login(services, username, password).await
	else {
		return authorize_login_error_response(
			*services,
			pending,
			&form.auth_id,
			"Wrong username or password.",
		)
		.await;
	};

	let created = services
		.oauth_provider
		.consent
		.create(user_id)
		.await;
	let client = services
		.oauth_provider
		.clients
		.get(&pending.client_id)
		.await
		.ok();
	let mut response =
		render_consent_page(pending, &form.auth_id, client.as_ref(), &created.session);
	let base_url = public_base_url(*services);
	append_consent_cookie(&mut response, &created, base_url.starts_with("https://"))?;

	Ok(response)
}

async fn authorize_login_error_response(
	services: crate::State,
	pending: &PendingAuthorization,
	auth_id: &str,
	error: &str,
) -> Result<Response> {
	let client = services
		.oauth_provider
		.clients
		.get(&pending.client_id)
		.await
		.ok();
	let base_url = public_base_url(services);
	let sso_links = sso_provider_links(services, &base_url, "/_tuwunel/oauth/authorize", &[(
		"auth_id", auth_id,
	)])?;

	Ok(render_login_page(pending, auth_id, client.as_ref(), &sso_links, Some(error)))
}

fn authorize_page_response(
	pending: &PendingAuthorization,
	auth_id: &str,
	client: Option<&RegisteredClient>,
	session: Option<&BrowserSession>,
	sso_links: &str,
) -> Response {
	if let Some(session) = session {
		render_consent_page(pending, auth_id, client, session)
	} else {
		render_login_page(pending, auth_id, client, sso_links, None)
	}
}

fn render_login_page(
	pending: &PendingAuthorization,
	auth_id: &str,
	client: Option<&RegisteredClient>,
	sso_links: &str,
	error: Option<&str>,
) -> Response {
	let error = error.map_or_else(String::new, |message| {
		format!("<p class=\"error\" role=\"alert\">{}</p>", escape_html(message),)
	});
	let body = LOGIN_TEMPLATE
		.replace("{{AUTH_ID}}", &escape_html(auth_id))
		.replace("{{CLIENT_NAME}}", &escape_html(&client_display_name(client, pending)))
		.replace("{{REDIRECT_URI}}", &escape_html(&pending.redirect_uri))
		.replace("{{DEVICE_ID}}", &escape_html(&pending.device_id))
		.replace("{{ERROR}}", &error)
		.replace("{{SSO_LINKS}}", sso_links);

	html_response(StatusCode::OK, body)
}

fn render_consent_page(
	pending: &PendingAuthorization,
	auth_id: &str,
	client: Option<&RegisteredClient>,
	session: &BrowserSession,
) -> Response {
	let body = CONSENT_TEMPLATE
		.replace("{{AUTH_ID}}", &escape_html(auth_id))
		.replace("{{CLIENT_NAME}}", &escape_html(&client_display_name(client, pending)))
		.replace("{{REDIRECT_URI}}", &escape_html(&pending.redirect_uri))
		.replace("{{DEVICE_ID}}", &escape_html(&pending.device_id))
		.replace("{{SCOPE}}", &escape_html(&human_scope(pending)))
		.replace("{{CSRF_TOKEN}}", &escape_html(&session.csrf_token))
		.replace("{{USER_ID}}", &escape_html(session.user_id.as_str()));

	html_response(StatusCode::OK, body)
}

fn authorization_success_redirect(
	redirect_uri: &str,
	code: &str,
	state: &str,
) -> Result<Response> {
	redirect_with_params(redirect_uri, &[("code", code), ("state", state)])
}

fn authorization_error_redirect(
	redirect_uri: &str,
	error: &str,
	state: &str,
) -> Result<Response> {
	redirect_with_params(redirect_uri, &[("error", error), ("state", state)])
}

fn redirect_with_params(redirect_uri: &str, params: &[(&str, &str)]) -> Result<Response> {
	let mut url = Url::parse(redirect_uri)?;
	{
		let mut query = url.query_pairs_mut();
		for (name, value) in params {
			query.append_pair(name, value);
		}
	}

	let mut response = StatusCode::FOUND.into_response();
	let location = HeaderValue::from_str(url.as_str())?;
	response.headers_mut().insert(LOCATION, location);

	Ok(response)
}

fn authorization_request_error_response(status: StatusCode) -> Response {
	message_response(
		status,
		"OAuth authorization request expired or invalid",
		"The OAuth authorization request is expired or invalid.",
	)
}

fn client_display_name(
	client: Option<&RegisteredClient>,
	pending: &PendingAuthorization,
) -> String {
	client
		.and_then(|client| client.metadata.client_name.as_deref())
		.unwrap_or(&pending.client_id)
		.to_owned()
}

fn human_scope(pending: &PendingAuthorization) -> String {
	format!("Matrix client API for device {}", pending.device_id)
}

fn authorize_disabled_error() -> Error {
	Error::Request(ErrorKind::Unrecognized, "Unrecognized request.".into(), StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
	use std::{
		path::PathBuf,
		sync::Arc,
		time::{SystemTime, UNIX_EPOCH},
	};

	use axum::{
		Router,
		body::{Body, to_bytes},
	};
	use http::{
		Request, StatusCode,
		header::{AUTHORIZATION, CONTENT_TYPE, COOKIE, LOCATION},
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
			clients::ClientRegistrationRequest, consent::BrowserSession, scope::parse_scope,
		},
	};
	use url::Url;

	use super::{
		AuthorizeAction, AuthorizePostForm, authorization_error_redirect,
		authorization_request_error_response, authorization_success_redirect,
		authorize_disabled_error, validated_authorize_decision,
	};

	#[tokio::test]
	async fn oauth_authorize_success_redirect_has_code_and_verbatim_state() {
		let response = authorization_success_redirect(
			"com.example.app:/oauth2redirect?existing=1",
			"auth-code",
			"opaque%20state+bytes",
		)
		.expect("redirect builds");
		let (parts, _) = response.into_parts();

		assert_eq!(parts.status, StatusCode::FOUND);
		assert_eq!(
			parts
				.headers
				.get(LOCATION)
				.and_then(|value| value.to_str().ok()),
			Some(
				"com.example.app:/oauth2redirect?existing=1&code=auth-code&state=opaque%\
				 2520state%2Bbytes"
			),
		);
	}

	#[tokio::test]
	async fn oauth_authorize_error_redirect_has_error_and_verbatim_state() {
		let response = authorization_error_redirect(
			"https://client.example/callback",
			"invalid_request",
			"state with spaces",
		)
		.expect("redirect builds");
		let (parts, _) = response.into_parts();

		assert_eq!(parts.status, StatusCode::FOUND);
		assert_eq!(
			parts
				.headers
				.get(LOCATION)
				.and_then(|value| value.to_str().ok()),
			Some("https://client.example/callback?error=invalid_request&state=state+with+spaces"),
		);
	}

	#[tokio::test]
	async fn oauth_authorize_local_error_page_is_html_and_does_not_redirect() {
		let response = authorization_request_error_response(StatusCode::BAD_REQUEST);
		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::BAD_REQUEST);
		assert!(parts.headers.get(LOCATION).is_none());
		assert!(
			parts
				.headers
				.get(CONTENT_TYPE)
				.and_then(|value| value.to_str().ok())
				.is_some_and(|value| value.starts_with("text/html")),
		);
		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		let body = std::str::from_utf8(&body).expect("body is utf8");
		assert!(body.contains("expired or invalid"));
	}

	#[test]
	fn oauth_authorize_approve_without_session_or_csrf_is_rejected() {
		let form = AuthorizePostForm {
			action: AuthorizeAction::Approve,
			auth_id: "auth-id".into(),
			csrf: Some("csrf".into()),
			username: None,
			password: None,
		};
		let error = validated_authorize_decision(&form, None).expect_err("session required");
		assert_eq!(error.status_code(), StatusCode::FORBIDDEN);

		let session = BrowserSession {
			user_id: owned_user_id!("@alice:example.com"),
			csrf_token: "expected".into(),
			expires: SystemTime::UNIX_EPOCH,
		};
		let error =
			validated_authorize_decision(&form, Some(&session)).expect_err("csrf mismatch");
		assert_eq!(error.status_code(), StatusCode::FORBIDDEN);
	}

	#[test]
	fn oauth_authorize_approval_uses_session_user_id() {
		let session = BrowserSession {
			user_id: owned_user_id!("@alice:example.com"),
			csrf_token: "csrf".into(),
			expires: SystemTime::UNIX_EPOCH,
		};
		let form = AuthorizePostForm {
			action: AuthorizeAction::Approve,
			auth_id: "auth-id".into(),
			csrf: Some("csrf".into()),
			username: None,
			password: None,
		};

		let decision =
			validated_authorize_decision(&form, Some(&session)).expect("decision validates");
		assert_eq!(decision.user_id, owned_user_id!("@alice:example.com"));
		assert_eq!(decision.auth_id, "auth-id");
		assert_eq!(decision.action, AuthorizeAction::Approve);
	}

	#[test]
	fn oauth_authorize_gate_off_error_is_unrecognized_404() {
		let error = authorize_disabled_error();
		assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
		assert_eq!(error.kind(), ruma::api::client::error::ErrorKind::Unrecognized,);
	}

	#[derive(Serialize)]
	struct AuthorizeGetForm<'a> {
		response_type: &'a str,
		client_id: &'a str,
		redirect_uri: &'a str,
		scope: &'a str,
		state: &'a str,
		code_challenge: &'a str,
		code_challenge_method: &'a str,
	}

	#[derive(Serialize)]
	struct AuthorizePostFormBody<'a> {
		action: &'a str,
		auth_id: &'a str,
		csrf: Option<&'a str>,
	}

	#[derive(Serialize)]
	struct AuthorizationCodeGrantForm<'a> {
		grant_type: &'a str,
		code: &'a str,
		redirect_uri: &'a str,
		client_id: &'a str,
		code_verifier: &'a str,
	}

	#[tokio::test]
	async fn oauth_authorize_get_valid_request_renders_login_page() -> tuwunel_core::Result {
		let (services, database_path) = test_services("oauth-authorize-get").await?;
		let registered = register_client(&services).await?;
		let (state, guard) = crate::router::state::create(services.clone());
		let app = crate::router::build(Router::new(), &services.server).with_state(state);
		let uri = authorize_uri(&AuthorizeGetForm {
			response_type: "code",
			client_id: &registered.client_id,
			redirect_uri: "http://127.0.0.1:1234/callback",
			scope: "urn:matrix:client:api:* urn:matrix:client:device:DEVICEAUTH",
			state: "state123",
			code_challenge: "challenge",
			code_challenge_method: "S256",
		});

		let (status, headers, body) = request_text(
			app,
			Request::builder()
				.method("GET")
				.uri(uri)
				.body(Body::empty())
				.expect("request builds"),
		)
		.await;

		assert_eq!(status, StatusCode::OK);
		assert!(headers.get(LOCATION).is_none());
		assert!(body.contains("name=\"auth_id\""));
		assert!(body.contains("DEVICEAUTH"));
		assert!(body.contains("http://127.0.0.1:1234/callback"));

		drop(guard);
		drop(services);
		std::fs::remove_dir_all(database_path).ok();
		Ok(())
	}

	#[tokio::test]
	async fn authorization_code_conformance_register_authorize_token_whoami()
	-> tuwunel_core::Result {
		let (services, database_path) = test_services("authorization-code-conformance").await?;
		let user_id = owned_user_id!("@oauth-conformance:localhost");
		services
			.users
			.create(&user_id, Some("password"), None)
			.await?;
		let (state, guard) = crate::router::state::create(services.clone());
		let app = crate::router::build(Router::new(), &services.server).with_state(state);

		let (status, _, registered) = request_json(
			app.clone(),
			Request::builder()
				.method("POST")
				.uri("/_tuwunel/oauth/register")
				.header(CONTENT_TYPE, "application/json")
				.body(Body::from(
					json!({
						"client_name": "OAuth conformance client",
						"client_uri": "https://client.example/",
						"redirect_uris": ["http://127.0.0.1/callback"],
						"grant_types": ["authorization_code", "refresh_token"],
						"response_types": ["code"],
						"token_endpoint_auth_method": "none"
					})
					.to_string(),
				))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::CREATED);
		let client_id = registered["client_id"]
			.as_str()
			.expect("client id returned");

		let authorize_uri = authorize_uri(&AuthorizeGetForm {
			response_type: "code",
			client_id,
			redirect_uri: "http://127.0.0.1:1234/callback",
			scope: "urn:matrix:client:api:* urn:matrix:client:device:DEVICEE2E",
			state: "state123",
			code_challenge: "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
			code_challenge_method: "S256",
		});
		let (status, _, authorize_page) = request_text(
			app.clone(),
			Request::builder()
				.method("GET")
				.uri(authorize_uri)
				.body(Body::empty())
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::OK);
		let auth_id = hidden_auth_id(&authorize_page);

		let created_session = services
			.oauth_provider
			.consent
			.create(user_id.clone())
			.await;
		let approve_body = serde_html_form::to_string(&AuthorizePostFormBody {
			action: "approve",
			auth_id: &auth_id,
			csrf: Some(&created_session.session.csrf_token),
		})?;
		let (status, headers, _) = request_text(
			app.clone(),
			Request::builder()
				.method("POST")
				.uri("/_tuwunel/oauth/authorize")
				.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
				.header(COOKIE, format!("tuwunel_oauth_consent={}", created_session.session_id))
				.body(Body::from(approve_body))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::FOUND);
		let location = headers
			.get(LOCATION)
			.and_then(|value| value.to_str().ok())
			.expect("approve redirects");
		let redirect = Url::parse(location).expect("location parses");
		assert_eq!(
			redirect
				.query_pairs()
				.find(|(name, _)| name == "state")
				.map(|(_, value)| value.into_owned()),
			Some("state123".into())
		);
		let code = redirect
			.query_pairs()
			.find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
			.expect("code is returned");

		let token_body = serde_html_form::to_string(&AuthorizationCodeGrantForm {
			grant_type: "authorization_code",
			code: &code,
			redirect_uri: "http://127.0.0.1:5678/callback",
			client_id,
			code_verifier: "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
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
		assert_eq!(found_device.as_str(), "DEVICEE2E");

		let (status, _, whoami) = request_json(
			app,
			Request::builder()
				.method("GET")
				.uri("/_matrix/client/v3/account/whoami")
				.header(AUTHORIZATION, format!("Bearer {access_token}"))
				.body(Body::empty())
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(whoami["user_id"], user_id.as_str());
		assert_eq!(whoami["device_id"], "DEVICEE2E");

		drop(guard);
		drop(services);
		std::fs::remove_dir_all(database_path).ok();
		Ok(())
	}

	#[tokio::test]
	async fn oauth_authorize_and_authorization_code_token_branch_are_gated_off()
	-> tuwunel_core::Result {
		let (services, database_path) =
			test_services_with_next_gen("oauth-authorize-gating", false).await?;
		let (state, guard) = crate::router::state::create(services.clone());
		let app = crate::router::build(Router::new(), &services.server).with_state(state);

		let (status, ..) = request_text(
			app.clone(),
			Request::builder()
				.method("GET")
				.uri("/_tuwunel/oauth/authorize")
				.body(Body::empty())
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::NOT_FOUND);

		let (status, ..) = request_text(
			app.clone(),
			Request::builder()
				.method("POST")
				.uri("/_tuwunel/oauth/authorize")
				.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
				.body(Body::from("action=approve&auth_id=missing"))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::NOT_FOUND);

		let token_body = serde_html_form::to_string(&AuthorizationCodeGrantForm {
			grant_type: "authorization_code",
			code: "code",
			redirect_uri: "https://client.example/callback",
			client_id: "client",
			code_verifier: "verifier",
		})?;
		let (status, ..) = request_text(
			app,
			Request::builder()
				.method("POST")
				.uri("/_tuwunel/oauth/token")
				.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
				.body(Body::from(token_body))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::NOT_FOUND);

		drop(guard);
		drop(services);
		std::fs::remove_dir_all(database_path).ok();
		Ok(())
	}

	#[tokio::test]
	async fn oauth_authorize_redirect_validation_errors_never_redirect() -> tuwunel_core::Result {
		let (services, database_path) =
			test_services("oauth-authorize-redirect-validation").await?;
		let (state, guard) = crate::router::state::create(services.clone());
		let app = crate::router::build(Router::new(), &services.server).with_state(state);
		let uri = authorize_uri(&AuthorizeGetForm {
			response_type: "code",
			client_id: "unknown-client",
			redirect_uri: "https://attacker.example/callback",
			scope: "urn:matrix:client:api:* urn:matrix:client:device:DEVICEAUTH",
			state: "state123",
			code_challenge: "challenge",
			code_challenge_method: "S256",
		});

		let (status, headers, body) = request_text(
			app,
			Request::builder()
				.method("GET")
				.uri(uri)
				.body(Body::empty())
				.expect("request builds"),
		)
		.await;

		assert_eq!(status, StatusCode::BAD_REQUEST);
		assert!(headers.get(LOCATION).is_none());
		assert!(body.contains("expired or invalid"));

		drop(guard);
		drop(services);
		std::fs::remove_dir_all(database_path).ok();
		Ok(())
	}

	#[tokio::test]
	async fn oauth_authorize_post_validation_errors_redirect_with_state() -> tuwunel_core::Result
	{
		let (services, database_path) = test_services("oauth-authorize-error-redirect").await?;
		let registered = register_client(&services).await?;
		let (state, guard) = crate::router::state::create(services.clone());
		let app = crate::router::build(Router::new(), &services.server).with_state(state);
		let uri = authorize_uri(&AuthorizeGetForm {
			response_type: "token",
			client_id: &registered.client_id,
			redirect_uri: "http://127.0.0.1:1234/callback",
			scope: "urn:matrix:client:api:* urn:matrix:client:device:DEVICEAUTH",
			state: "state123",
			code_challenge: "challenge",
			code_challenge_method: "S256",
		});

		let (status, headers, _) = request_text(
			app,
			Request::builder()
				.method("GET")
				.uri(uri)
				.body(Body::empty())
				.expect("request builds"),
		)
		.await;

		assert_eq!(status, StatusCode::FOUND);
		assert_eq!(
			headers
				.get(LOCATION)
				.and_then(|value| value.to_str().ok()),
			Some("http://127.0.0.1:1234/callback?error=unsupported_response_type&state=state123"),
		);

		drop(guard);
		drop(services);
		std::fs::remove_dir_all(database_path).ok();
		Ok(())
	}

	#[tokio::test]
	async fn oauth_authorize_approve_requires_csrf_and_mints_code() -> tuwunel_core::Result {
		let (services, database_path) = test_services("oauth-authorize-approve").await?;
		let registered = register_client(&services).await?;
		let user_id = owned_user_id!("@alice:localhost");
		let created_session = services
			.oauth_provider
			.consent
			.create(user_id.clone())
			.await;
		let pending = services
			.oauth_provider
			.authorize
			.pending
			.create(
				registered.client_id.clone(),
				"http://127.0.0.1:1234/callback",
				parse_scope("urn:matrix:client:api:* urn:matrix:client:device:DEVICEAUTH")?,
				"challenge",
				"state123",
			)
			.await?;
		let (state, guard) = crate::router::state::create(services.clone());
		let app = crate::router::build(Router::new(), &services.server).with_state(state);

		let forbidden_body = serde_html_form::to_string(&AuthorizePostFormBody {
			action: "approve",
			auth_id: &pending.auth_id,
			csrf: Some(&created_session.session.csrf_token),
		})?;
		let (status, _, body) = request_text(
			app.clone(),
			Request::builder()
				.method("POST")
				.uri("/_tuwunel/oauth/authorize")
				.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
				.body(Body::from(forbidden_body))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::FORBIDDEN);
		assert!(body.contains("Approval session expired"));

		let wrong_csrf_body = serde_html_form::to_string(&AuthorizePostFormBody {
			action: "approve",
			auth_id: &pending.auth_id,
			csrf: Some("wrong"),
		})?;
		let (status, ..) = request_text(
			app.clone(),
			Request::builder()
				.method("POST")
				.uri("/_tuwunel/oauth/authorize")
				.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
				.header(COOKIE, format!("tuwunel_oauth_consent={}", created_session.session_id))
				.body(Body::from(wrong_csrf_body))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::FORBIDDEN);

		let approve_body = serde_html_form::to_string(&AuthorizePostFormBody {
			action: "approve",
			auth_id: &pending.auth_id,
			csrf: Some(&created_session.session.csrf_token),
		})?;
		let (status, headers, _) = request_text(
			app,
			Request::builder()
				.method("POST")
				.uri("/_tuwunel/oauth/authorize")
				.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
				.header(COOKIE, format!("tuwunel_oauth_consent={}", created_session.session_id))
				.body(Body::from(approve_body))
				.expect("request builds"),
		)
		.await;
		assert_eq!(status, StatusCode::FOUND);
		let location = headers
			.get(LOCATION)
			.and_then(|value| value.to_str().ok())
			.expect("approve redirects");
		assert!(location.starts_with("http://127.0.0.1:1234/callback?code="));
		assert!(location.ends_with("&state=state123"));
		let code = Url::parse(location)
			.expect("location parses")
			.query_pairs()
			.find_map(|(name, value)| (name == "code").then(|| value.into_owned()))
			.expect("code is returned");
		let authorization = services
			.oauth_provider
			.authorize
			.codes
			.consume(&code)
			.await?;
		assert_eq!(authorization.user_id, user_id);
		assert_eq!(authorization.device_id, "DEVICEAUTH");

		drop(guard);
		drop(services);
		std::fs::remove_dir_all(database_path).ok();
		Ok(())
	}

	#[tokio::test]
	async fn oauth_authorize_deny_redirects_access_denied_with_state() -> tuwunel_core::Result {
		let (services, database_path) = test_services("oauth-authorize-deny").await?;
		let registered = register_client(&services).await?;
		let pending = services
			.oauth_provider
			.authorize
			.pending
			.create(
				registered.client_id,
				"http://127.0.0.1:1234/callback",
				parse_scope("urn:matrix:client:api:* urn:matrix:client:device:DEVICEDENY")?,
				"challenge",
				"state123",
			)
			.await?;
		let (state, guard) = crate::router::state::create(services.clone());
		let app = crate::router::build(Router::new(), &services.server).with_state(state);
		let deny_body = serde_html_form::to_string(&AuthorizePostFormBody {
			action: "deny",
			auth_id: &pending.auth_id,
			csrf: None,
		})?;

		let (status, headers, _) = request_text(
			app,
			Request::builder()
				.method("POST")
				.uri("/_tuwunel/oauth/authorize")
				.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
				.body(Body::from(deny_body))
				.expect("request builds"),
		)
		.await;

		assert_eq!(status, StatusCode::FOUND);
		assert_eq!(
			headers
				.get(LOCATION)
				.and_then(|value| value.to_str().ok()),
			Some("http://127.0.0.1:1234/callback?error=access_denied&state=state123"),
		);

		drop(guard);
		drop(services);
		std::fs::remove_dir_all(database_path).ok();
		Ok(())
	}

	async fn register_client(
		services: &Arc<Services>,
	) -> tuwunel_core::Result<tuwunel_service::oauth_provider::clients::RegisteredClient> {
		services
			.oauth_provider
			.clients
			.register(ClientRegistrationRequest {
				client_name: Some("Element X".into()),
				client_uri: Some("https://client.example/".into()),
				redirect_uris: vec!["http://127.0.0.1/callback".into()],
				grant_types: vec!["authorization_code".into(), "refresh_token".into()],
				response_types: vec!["code".into()],
				..Default::default()
			})
			.await
	}

	fn authorize_uri(form: &AuthorizeGetForm<'_>) -> String {
		format!(
			"/_tuwunel/oauth/authorize?{}",
			serde_html_form::to_string(form).expect("form serializes"),
		)
	}

	async fn request_text(
		app: Router,
		request: Request<Body>,
	) -> (StatusCode, http::HeaderMap, String) {
		let response = app
			.oneshot(request)
			.await
			.expect("router handles request");
		let (parts, body) = response.into_parts();
		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		let body = std::str::from_utf8(&body)
			.expect("body is utf8")
			.to_owned();

		(parts.status, parts.headers, body)
	}

	async fn request_json(
		app: Router,
		request: Request<Body>,
	) -> (StatusCode, http::HeaderMap, JsonValue) {
		let (status, headers, body) = request_text(app, request).await;
		let json = serde_json::from_str(&body).expect("response body is json");

		(status, headers, json)
	}

	fn hidden_auth_id(body: &str) -> String {
		let marker = "name=\"auth_id\" value=\"";
		body.split_once(marker)
			.and_then(|(_, rest)| rest.split_once('"'))
			.map(|(value, _)| value.to_owned())
			.expect("auth_id input exists")
	}

	async fn test_services(name: &str) -> tuwunel_core::Result<(Arc<Services>, PathBuf)> {
		test_services_with_next_gen(name, true).await
	}

	async fn test_services_with_next_gen(
		name: &str,
		next_gen_auth: bool,
	) -> tuwunel_core::Result<(Arc<Services>, PathBuf)> {
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
			.merge(("next_gen_auth", next_gen_auth))
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
