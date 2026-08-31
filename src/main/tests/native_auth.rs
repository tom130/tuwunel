#![cfg(test)]

use std::{
	fs::remove_dir_all,
	process::id as process_id,
	sync::Arc,
	time::{Duration, SystemTime},
};

use axum::{Router, body::Body, response::Response};
use http::{
	Request, StatusCode,
	header::{CONTENT_SECURITY_POLICY, CONTENT_TYPE, LOCATION},
};
use http_body_util::BodyExt;
use tokio::sync::Barrier;
use tower::ServiceExt;
use tuwunel::{Args, Runtime, Server};
use tuwunel_core::{Err, Error, Result, ruma::UserId};
use tuwunel_service::{
	Services,
	oauth::server::{AuthRequest, DcrRequest},
	users::Register,
};

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

	let auth_request = test_auth_request();
	assert_auth_request_storage(services, &auth_request).await?;
	assert_concurrent_auth_request_take(services, &auth_request).await?;
	assert_native_pages(services, &auth_request).await?;

	let user_id = UserId::parse_with_server_name("nativealice", services.globals.server_name())?;
	services
		.users
		.full_register(Register {
			user_id: Some(&user_id),
			password: Some("a-strong-test-password"),
			..Default::default()
		})
		.await?;

	assert_cross_origin_authorization_flow(services).await?;
	assert_completion_error_pages(services, &auth_request, &user_id).await?;
	assert_authorize_error_pages(services).await?;

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

async fn assert_concurrent_auth_request_take(
	services: &Arc<Services>,
	auth_request: &AuthRequest,
) -> Result {
	const CONTENDERS: usize = 32;

	let oidc = services.oauth.get_server()?;
	// Widen the interval between the database read and remove enough for every
	// contender to observe the same value on a multi-threaded runtime.
	let mut large_auth_request = auth_request.clone();
	large_auth_request.redirect_uri = format!("https://element.example/{}", "x".repeat(4 << 20));
	oidc.store_auth_request("native-concurrent-take", &large_auth_request);

	let barrier = Arc::new(Barrier::new(CONTENDERS));
	let mut tasks = Vec::with_capacity(CONTENDERS);
	for _ in 0..CONTENDERS {
		let services = services.clone();
		let barrier = barrier.clone();
		tasks.push(tokio::spawn(async move {
			barrier.wait().await;
			services
				.oauth
				.get_server()
				.expect("OIDC server must remain available")
				.take_auth_request("native-concurrent-take")
				.await
				.is_ok()
		}));
	}

	let mut successes: usize = 0;
	for task in tasks {
		let succeeded = task
			.await
			.expect("concurrent take task must not panic");
		successes = successes.saturating_add(usize::from(succeeded));
	}

	if successes != 1 {
		return Err!("authorization request was taken {successes} times concurrently");
	}

	Ok(())
}

async fn assert_cross_origin_authorization_flow(services: &Arc<Services>) -> Result {
	let oidc = services.oauth.get_server()?;
	let redirect_uri = "https://element-cross-origin.example/callback";
	let client = oidc
		.register_client(DcrRequest {
			redirect_uris: vec![redirect_uri.to_owned()],
			client_name: None,
			client_uri: None,
			logo_uri: None,
			contacts: Vec::new(),
			token_endpoint_auth_method: None,
			grant_types: None,
			response_types: None,
			application_type: None,
			policy_uri: None,
			tos_uri: None,
			software_id: None,
			software_version: None,
		})
		.await?;
	let request = Request::get(format!(
		"/_tuwunel/oidc/authorize?client_id={}&redirect_uri=https%3A%2F%2Felement-cross-origin.\
		 example%2Fcallback&response_type=code&scope=openid&code_challenge=test-challenge&\
		 code_challenge_method=S256",
		client.client_id
	))
	.header("x-forwarded-for", "127.0.0.1")
	.body(Body::empty())
	.expect("the cross-origin authorize URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
	let native_location = response_location(&response);
	assert!(native_location.contains("/_tuwunel/oidc/native?"));
	let req_id = native_location
		.split("oidc_req_id=")
		.nth(1)
		.and_then(|query| query.split('&').next())
		.expect("authorize redirect must contain oidc_req_id");

	let request = Request::get(&native_location)
		.body(Body::empty())
		.expect("the native redirect URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::OK);
	assert_eq!(
		response.headers()[CONTENT_SECURITY_POLICY],
		"default-src 'none';script-src 'self';style-src 'self';frame-ancestors 'none';form-action 'self' https://element-cross-origin.example;base-uri 'none'"
	);

	let request = Request::post("/_tuwunel/oidc/native")
		.header(CONTENT_TYPE, "application/x-www-form-urlencoded")
		.header("x-forwarded-for", "127.0.0.1")
		.body(Body::from(format!(
			"oidc_req_id={req_id}&username=nativealice&password=a-strong-test-password"
		)))
		.expect("the native credential submission must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::SEE_OTHER);
	let complete_location = response_location(&response);
	assert!(complete_location.contains("/_tuwunel/oidc/_complete?"));

	let request = Request::get(complete_location)
		.body(Body::empty())
		.expect("the completion redirect URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
	let callback_location = response_location(&response);
	assert!(callback_location.starts_with(&format!("{redirect_uri}?code=")));

	Ok(())
}

fn test_auth_request() -> AuthRequest {
	let now = SystemTime::now();
	AuthRequest {
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
		expires_at: now
			.checked_add(Duration::from_mins(1))
			.unwrap_or(now),
	}
}

async fn assert_auth_request_storage(
	services: &Arc<Services>,
	auth_request: &AuthRequest,
) -> Result {
	let oidc = services.oauth.get_server()?;
	oidc.store_auth_request("native-peek-test", auth_request);
	assert_eq!(
		oidc.peek_auth_request("native-peek-test")
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

	let now = SystemTime::now();
	let mut expired_request = auth_request.clone();
	expired_request.expires_at = now
		.checked_sub(Duration::from_secs(1))
		.unwrap_or(SystemTime::UNIX_EPOCH);
	oidc.store_auth_request("native-expired-test", &expired_request);
	let expired_error = match oidc
		.peek_auth_request("native-expired-test")
		.await
	{
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

	Ok(())
}

async fn assert_native_pages(services: &Arc<Services>, auth_request: &AuthRequest) -> Result {
	let oidc = services.oauth.get_server()?;
	oidc.store_auth_request("native-csp-test", auth_request);
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

	let request =
		Request::get("/_tuwunel/oidc/native?oidc_req_id=unknown-native-request&view=login")
			.body(Body::empty())
			.expect("the static unknown-native URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::NOT_FOUND);
	assert_html_response(&response);
	let body = response_text(response).await;
	assert!(body.contains("sign-in link has expired"));
	assert!(!body.contains("<form"));

	Ok(())
}

async fn assert_completion_error_pages(
	services: &Arc<Services>,
	auth_request: &AuthRequest,
	user_id: &UserId,
) -> Result {
	let oidc = services.oauth.get_server()?;
	let retry_token = "native-completion-retry-token";
	let _expires_in = services
		.users
		.create_login_token(user_id, retry_token);
	oidc.store_auth_request("native-completion-retry", auth_request);
	let request = Request::get(
		"/_tuwunel/oidc/_complete?oidc_req_id=native-completion-retry&loginToken=invalid",
	)
	.body(Body::empty())
	.expect("the static bad-token completion URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::FORBIDDEN);
	assert_html_response(&response);
	let body = response_text(response).await;
	assert!(body.contains("Invalid or expired login token"));
	assert!(body.contains(r#"href="https://element.example""#));
	assert!(!body.contains("errcode"));

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
		.create_login_token(user_id, preserved_token);
	let request = Request::get(format!(
		"/_tuwunel/oidc/_complete?oidc_req_id=unknown-request&loginToken={preserved_token}"
	))
	.body(Body::empty())
	.expect("the unknown-request completion URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::NOT_FOUND);
	assert_html_response(&response);
	let body = response_text(response).await;
	assert!(body.contains("Unknown or expired authorization request"));
	assert!(body.contains(r#"href="/_tuwunel/oidc/account""#));
	assert!(!body.contains("errcode"));

	oidc.store_auth_request("native-completion-after-unknown", auth_request);
	let request = Request::get(format!(
		"/_tuwunel/oidc/_complete?oidc_req_id=native-completion-after-unknown&\
		 loginToken={preserved_token}"
	))
	.body(Body::empty())
	.expect("the preserved-token completion URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);

	Ok(())
}

async fn assert_authorize_error_pages(services: &Arc<Services>) -> Result {
	let oidc = services.oauth.get_server()?;
	let client = oidc
		.register_client(DcrRequest {
			redirect_uris: vec!["https://element.example/callback?query=value".to_owned()],
			client_name: None,
			client_uri: None,
			logo_uri: None,
			contacts: Vec::new(),
			token_endpoint_auth_method: None,
			grant_types: None,
			response_types: None,
			application_type: None,
			policy_uri: None,
			tos_uri: None,
			software_id: None,
			software_version: None,
		})
		.await?;
	let request = Request::get(format!(
		"/_tuwunel/oidc/authorize?client_id={}&redirect_uri=https%3A%2F%2Felement.example%\
		 2Fcallback%3Fquery%3Dvalue&response_type=code&scope=openid&\
		 code_challenge=test-challenge&code_challenge_method=S256&idp_id=missing-provider",
		client.client_id
	))
	.header("x-forwarded-for", "127.0.0.1")
	.body(Body::empty())
	.expect("the registered-client authorize URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::BAD_REQUEST);
	assert_html_response(&response);
	let body = response_text(response).await;
	assert!(body.contains("Unrecognized identity provider"));
	assert!(body.contains(r#"href="https://element.example""#));
	assert!(!body.contains("errcode"));

	let request = Request::get(
		"/_tuwunel/oidc/authorize?client_id=unknown-client&redirect_uri=https%3A%2F%2Fevil.\
		 example%2Fcallback&response_type=code&scope=openid&code_challenge=test-challenge&\
		 code_challenge_method=S256",
	)
	.header("x-forwarded-for", "127.0.0.1")
	.body(Body::empty())
	.expect("the unknown-client authorize URI must be valid");
	let response = api_request(services, request).await;
	assert_eq!(response.status(), StatusCode::NOT_FOUND);
	assert_html_response(&response);
	let body = response_text(response).await;
	assert!(body.contains("Unknown client_id"));
	assert!(body.contains(r#"href="/_tuwunel/oidc/account""#));
	assert!(!body.contains("evil.example"));
	assert!(!body.contains("errcode"));

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

fn assert_html_response(response: &Response) {
	let content_type = response
		.headers()
		.get(CONTENT_TYPE)
		.and_then(|value| value.to_str().ok())
		.unwrap_or_default();

	assert!(content_type.starts_with("text/html"));
}

fn response_location(response: &Response) -> String {
	response
		.headers()
		.get(LOCATION)
		.and_then(|value| value.to_str().ok())
		.expect("redirect response must have a valid Location header")
		.to_owned()
}

async fn response_text(response: Response) -> String {
	let body = response
		.into_body()
		.collect()
		.await
		.expect("test response body must be readable")
		.to_bytes();

	String::from_utf8(body.to_vec()).expect("test response body must be UTF-8")
}
