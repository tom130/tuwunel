#![cfg(test)]

use std::{
	fs::remove_dir_all,
	process::id as process_id,
	sync::Arc,
	time::{Duration, SystemTime},
};

use axum::{Router, body::Body, response::Response};
use http::{Request, StatusCode, header::CONTENT_SECURITY_POLICY};
use tower::ServiceExt;
use tuwunel::{Args, Runtime, Server};
use tuwunel_core::{Err, Error, Result, ruma::UserId};
use tuwunel_service::{Services, oauth::server::AuthRequest, users::Register};

/// The OIDC server (next-gen auth) constructs in native mode with no
/// third-party `identity_provider`, and the login-token tail the native handler
/// relies on round-trips for a freshly registered local account.
#[test]
fn native_oidc_serves_local_accounts() -> Result {
	// Isolate the database under /tmp so parallel test binaries do not contend.
	let db_path = format!("/tmp/tuwunel-test-native-auth-{}", process_id());

	let mut args = Args::default_test(&["fresh", "cleanup"]);
	args.maintenance = true;
	args.option.extend([
		format!("database_path=\"{db_path}\""),
		"well_known.client=\"https://localhost\"".to_owned(),
		"oidc_native_auth=true".to_owned(),
	]);

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;

	let result: Result = runtime.block_on(async {
		let services = tuwunel::async_start(&server).await?;

		let outcome = native_round_trip(&services).await;

		server.server.shutdown()?;
		drop(services);

		tuwunel::async_run(&server).await?;
		tuwunel::async_stop(&server).await?;

		outcome
	});

	drop(runtime);

	remove_dir_all(&db_path).ok();

	result
}

async fn native_round_trip(services: &Arc<Services>) -> Result {
	// The dependency on identity_provider is broken: the OIDC server is up.
	let oidc = services.oauth.get_server()?;

	let issuer = oidc.issuer_url()?;
	if !issuer.starts_with("https://localhost") {
		return Err!("unexpected issuer: {issuer}");
	}

	let now = SystemTime::now();
	let auth_request = AuthRequest {
		client_id: "native-test-client".to_owned(),
		redirect_uri: "https://element.example/callback".to_owned(),
		scope: "openid".to_owned(),
		state: None,
		nonce: None,
		code_challenge: None,
		code_challenge_method: None,
		idp_id: None,
		response_mode: None,
		created_at: now,
		expires_at: now.checked_add(Duration::from_mins(1)).unwrap_or(now),
	};
	oidc.store_auth_request("native-peek-test", &auth_request);
	assert_eq!(
		oidc
			.peek_auth_request("native-peek-test")
			.await?
			.redirect_uri,
		auth_request.redirect_uri
	);
	let _second_peek = oidc.peek_auth_request("native-peek-test").await?;
	let _taken_request = oidc.take_auth_request("native-peek-test").await?;
	let missing_error = match oidc.peek_auth_request("native-peek-test").await {
		| Ok(_) => return Err!("taken authorization request remained readable"),
		| Err(error) => error,
	};
	let missing_message = match missing_error {
		| Error::Request(_, message, _) => message,
		| other => return Err!("unexpected missing-request error: {other}"),
	};
	if missing_message != "Unknown or expired authorization request" {
		return Err!("unexpected missing-request message: {missing_message}");
	}

	let mut expired_request = auth_request.clone();
	expired_request.expires_at = now
		.checked_sub(Duration::from_secs(1))
		.unwrap_or(SystemTime::UNIX_EPOCH);
	oidc.store_auth_request("native-expired-test", &expired_request);
	let expired_error = match oidc.peek_auth_request("native-expired-test").await {
		| Ok(_) => return Err!("expired authorization request remained readable"),
		| Err(error) => error,
	};
	let expired_message = match expired_error {
		| Error::Request(_, message, _) => message,
		| other => return Err!("unexpected expired-request error: {other}"),
	};
	if expired_message != "Authorization request has expired" {
		return Err!("unexpected expired-request message: {expired_message}");
	}

	oidc.store_auth_request("native-csp-test", &auth_request);
	let request = Request::get("/_tuwunel/oidc/native?oidc_req_id=native-csp-test&view=login")
		.body(Body::empty())
		.expect("the static native test URI must be valid");
	let response = api_request(services, request).await;

	assert_eq!(response.status(), StatusCode::OK);
	let csp = response
		.headers()
		.get(CONTENT_SECURITY_POLICY)
		.and_then(|value| value.to_str().ok())
		.unwrap_or_default();
	if csp
		!= "default-src 'none';script-src 'self';style-src 'self';frame-ancestors 'none';form-action 'self' https://element.example;base-uri 'none'"
	{
		return Err!("unexpected native authorization CSP: {csp}");
	}

	let user_id = UserId::parse_with_server_name("nativealice", services.globals.server_name())?;
	services
		.users
		.full_register(Register {
			user_id: Some(&user_id),
			password: Some("a-strong-test-password"),
			..Default::default()
		})
		.await?;

	let retry_token = "native-completion-retry-token";
	let _expires_in = services.users.create_login_token(&user_id, retry_token);
	oidc.store_auth_request("native-completion-retry", &auth_request);
	let request = Request::get(
		"/_tuwunel/oidc/_complete?oidc_req_id=native-completion-retry&loginToken=invalid",
	)
	.body(Body::empty())
	.expect("the static bad-token completion URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::FORBIDDEN);

	let request = Request::get(format!(
		"/_tuwunel/oidc/_complete?oidc_req_id=native-completion-retry&loginToken={retry_token}"
	))
	.body(Body::empty())
	.expect("the retry completion URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);

	let preserved_token = "native-completion-preserved-token";
	let _expires_in = services
		.users
		.create_login_token(&user_id, preserved_token);
	let request = Request::get(format!(
		"/_tuwunel/oidc/_complete?oidc_req_id=unknown-request&loginToken={preserved_token}"
	))
	.body(Body::empty())
	.expect("the unknown-request completion URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::NOT_FOUND);

	oidc.store_auth_request("native-completion-after-unknown", &auth_request);
	let request = Request::get(format!(
		"/_tuwunel/oidc/_complete?oidc_req_id=native-completion-after-unknown&loginToken={preserved_token}"
	))
	.body(Body::empty())
	.expect("the preserved-token completion URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);

	// The native submit handler authenticates, mints a login token, and lets
	// _complete consume it; exercise that token tail directly.
	let token = "native-auth-test-login-token";
	let _expires_in = services.users.create_login_token(&user_id, token);
	let resolved = services
		.users
		.find_from_login_token(token)
		.await?;

	if resolved != user_id {
		return Err!("login token resolved to the wrong user: {resolved}");
	}

	Ok(())
}

async fn api_request(services: &Arc<Services>, request: Request<Body>) -> Response {
	let (state, state_guard) = tuwunel_api::router::state::create(services.clone());
	let router = tuwunel_api::router::build(
		Router::<tuwunel_api::router::state::State>::new(),
		&services.server,
	)
	.with_state(state);
	let response = router
		.oneshot(request)
		.await
		.expect("the API router service is infallible");

	drop(state_guard);

	response
}
