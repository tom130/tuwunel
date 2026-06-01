use axum::{extract::State, response::Response};
use axum_extra::extract::cookie::CookieJar;
use bytes::Bytes;
use http::{StatusCode, Uri};
use ruma::{OwnedUserId, api::client::error::ErrorKind};
use serde::Deserialize;
use tuwunel_core::{Err, Error, Result, err};
use tuwunel_service::oauth_provider::{
	consent::BrowserSession,
	grants::{GrantStatus, PendingGrant},
};

use super::web::{
	append_consent_cookie, consent_session_from_cookie, escape_html, html_response,
	message_response, public_base_url, sso_provider_links,
};

const LOGIN_TEMPLATE: &str = include_str!("templates/consent_login.html");
const CONSENT_TEMPLATE: &str = include_str!("templates/consent_approval.html");

#[derive(Deserialize)]
struct LinkQuery {
	user_code: Option<String>,
	#[serde(rename = "loginToken")]
	login_token: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum LinkAction {
	Login,
	Approve,
	Deny,
}

#[derive(Debug, Deserialize)]
struct LinkPostForm {
	action: LinkAction,
	user_code: String,
	csrf: Option<String>,
	username: Option<String>,
	password: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
struct ConsentDecision {
	action: LinkAction,
	user_code: String,
	user_id: OwnedUserId,
}

/// # `GET /_tuwunel/oauth/link`
///
/// Browser-facing consent UI for approving a pending QR-login device grant.
pub(crate) async fn get_oauth_link_route(
	State(services): State<crate::State>,
	jar: CookieJar,
	uri: Uri,
) -> Result<Response> {
	if !services.server.config.next_gen_auth {
		return Err(consent_disabled_error());
	}

	let query: LinkQuery = serde_html_form::from_str(uri.query().unwrap_or_default())?;
	let Some(user_code) = query
		.user_code
		.as_deref()
		.map(str::trim)
		.filter(|user_code| !user_code.is_empty())
	else {
		return Ok(user_code_error_response(StatusCode::BAD_REQUEST));
	};

	let Ok(grant) = services
		.oauth_provider
		.grants
		.by_user_code(user_code)
		.await
	else {
		return Ok(user_code_error_response(StatusCode::NOT_FOUND));
	};

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
	let sso_links = sso_provider_links(services, &base_url, "/_tuwunel/oauth/link", &[(
		"user_code",
		user_code,
	)])?;
	let mut response = link_page_response(&grant, session.as_ref(), &sso_links);

	if let Some(created) = created_session.as_ref() {
		append_consent_cookie(&mut response, created, base_url.starts_with("https://"))?;
	}

	Ok(response)
}

/// # `POST /_tuwunel/oauth/link`
///
/// Handles browser login, approval, and denial form submissions for QR login.
pub(crate) async fn post_oauth_link_route(
	State(services): State<crate::State>,
	jar: CookieJar,
	body: Bytes,
) -> Result<Response> {
	if !services.server.config.next_gen_auth {
		return Err(consent_disabled_error());
	}

	let body = std::str::from_utf8(&body)?;
	let form: LinkPostForm = serde_html_form::from_str(body)?;
	let Ok(grant) = services
		.oauth_provider
		.grants
		.by_user_code(&form.user_code)
		.await
	else {
		return Ok(user_code_error_response(StatusCode::NOT_FOUND));
	};

	match form.action {
		| LinkAction::Login => password_login_response(&services, &form, &grant).await,
		| LinkAction::Approve | LinkAction::Deny => {
			let session = consent_session_from_cookie(&services, &jar).await;
			let Ok(decision) = validated_consent_decision(&form, session.as_ref()) else {
				return Ok(message_response(
					StatusCode::FORBIDDEN,
					"Approval session expired",
					"Open the QR-login approval link again and sign in before approving.",
				));
			};

			match decision.action {
				| LinkAction::Approve => {
					services
						.oauth_provider
						.grants
						.approve(&decision.user_code, decision.user_id)
						.await?;
					Ok(message_response(
						StatusCode::OK,
						"QR login approved",
						"You can return to the device that showed the QR code.",
					))
				},
				| LinkAction::Deny => {
					services
						.oauth_provider
						.grants
						.deny(&decision.user_code)
						.await?;
					Ok(message_response(
						StatusCode::OK,
						"QR login denied",
						"The device-code request has been denied.",
					))
				},
				| LinkAction::Login => unreachable!("login action handled above"),
			}
		},
	}
}

async fn password_login_response(
	services: &crate::State,
	form: &LinkPostForm,
	grant: &PendingGrant,
) -> Result<Response> {
	let Some(username) = form
		.username
		.as_deref()
		.map(str::trim)
		.filter(|username| !username.is_empty())
	else {
		return Ok(login_error_response(grant, "Username is required."));
	};
	let Some(password) = form.password.as_deref() else {
		return Ok(login_error_response(grant, "Password is required."));
	};

	let Ok(user_id) =
		crate::client::session::authenticate_password_login(services, username, password).await
	else {
		return Ok(login_error_response(grant, "Wrong username or password."));
	};

	let created = services
		.oauth_provider
		.consent
		.create(user_id)
		.await;
	let base_url = public_base_url(*services);
	let sso_links = sso_provider_links(*services, &base_url, "/_tuwunel/oauth/link", &[(
		"user_code",
		form.user_code.as_str(),
	)])?;
	let mut response = link_page_response(grant, Some(&created.session), &sso_links);
	append_consent_cookie(&mut response, &created, base_url.starts_with("https://"))?;

	Ok(response)
}

fn validated_consent_decision(
	form: &LinkPostForm,
	session: Option<&BrowserSession>,
) -> Result<ConsentDecision> {
	let session = session.ok_or_else(|| err!(Request(Forbidden("Consent session required."))))?;
	let csrf = form
		.csrf
		.as_deref()
		.ok_or_else(|| err!(Request(Forbidden("CSRF token required."))))?;

	if csrf != session.csrf_token {
		return Err!(Request(Forbidden("Invalid CSRF token.")));
	}

	Ok(ConsentDecision {
		action: form.action,
		user_code: form.user_code.clone(),
		user_id: session.user_id.clone(),
	})
}

fn link_page_response(
	grant: &PendingGrant,
	session: Option<&BrowserSession>,
	sso_links: &str,
) -> Response {
	match &grant.status {
		| GrantStatus::Denied => message_response(
			StatusCode::OK,
			"QR login denied",
			"This device-code request has already been denied.",
		),
		| GrantStatus::Approved { .. } => message_response(
			StatusCode::OK,
			"QR login approved",
			"This device-code request has already been approved.",
		),
		| GrantStatus::Pending =>
			if let Some(session) = session {
				render_consent_page(grant, session)
			} else {
				render_login_page(grant, sso_links, None)
			},
	}
}

fn render_login_page(grant: &PendingGrant, sso_links: &str, error: Option<&str>) -> Response {
	let error = error.map_or_else(String::new, |message| {
		format!("<p class=\"error\" role=\"alert\">{}</p>", escape_html(message),)
	});
	let body = LOGIN_TEMPLATE
		.replace("{{USER_CODE}}", &escape_html(&grant.user_code))
		.replace("{{DEVICE_ID}}", &escape_html(&grant.device_id))
		.replace("{{ERROR}}", &error)
		.replace("{{SSO_LINKS}}", sso_links);

	html_response(StatusCode::OK, body)
}

fn render_consent_page(grant: &PendingGrant, session: &BrowserSession) -> Response {
	let body = CONSENT_TEMPLATE
		.replace("{{USER_CODE}}", &escape_html(&grant.user_code))
		.replace("{{DEVICE_ID}}", &escape_html(&grant.device_id))
		.replace("{{SCOPE}}", &escape_html(&human_scope(grant)))
		.replace("{{CSRF_TOKEN}}", &escape_html(&session.csrf_token))
		.replace("{{USER_ID}}", &escape_html(session.user_id.as_str()));

	html_response(StatusCode::OK, body)
}

fn login_error_response(grant: &PendingGrant, error: &str) -> Response {
	render_login_page(grant, "", Some(error))
}

fn user_code_error_response(status: StatusCode) -> Response {
	message_response(
		status,
		"QR login link expired or invalid",
		"The QR-login approval code is expired or invalid.",
	)
}

fn human_scope(grant: &PendingGrant) -> String {
	format!("Matrix client API for device {}", grant.device_id)
}

fn consent_disabled_error() -> Error {
	Error::Request(ErrorKind::Unrecognized, "Unrecognized request.".into(), StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
	use axum::body::to_bytes;
	use http::StatusCode;
	use ruma::owned_user_id;
	use tuwunel_service::oauth_provider::consent::BrowserSession;

	use super::{
		LinkAction, LinkPostForm, consent_disabled_error, user_code_error_response,
		validated_consent_decision,
	};

	#[test]
	fn oauth_consent_approve_without_session_is_rejected() {
		let form = LinkPostForm {
			action: LinkAction::Approve,
			user_code: "ABCD-EFGH".into(),
			csrf: Some("csrf".into()),
			username: None,
			password: None,
		};

		let error = validated_consent_decision(&form, None).expect_err("session required");
		assert_eq!(error.status_code(), StatusCode::FORBIDDEN);
	}

	#[test]
	fn oauth_consent_csrf_mismatch_is_rejected() {
		let session = BrowserSession {
			user_id: owned_user_id!("@alice:example.com"),
			csrf_token: "expected".into(),
			expires: std::time::SystemTime::UNIX_EPOCH,
		};
		let form = LinkPostForm {
			action: LinkAction::Approve,
			user_code: "ABCD-EFGH".into(),
			csrf: Some("wrong".into()),
			username: None,
			password: None,
		};

		let error =
			validated_consent_decision(&form, Some(&session)).expect_err("csrf must match");
		assert_eq!(error.status_code(), StatusCode::FORBIDDEN);
	}

	#[test]
	fn oauth_consent_approval_uses_session_user_id() {
		let session = BrowserSession {
			user_id: owned_user_id!("@alice:example.com"),
			csrf_token: "csrf".into(),
			expires: std::time::SystemTime::UNIX_EPOCH,
		};
		let form = LinkPostForm {
			action: LinkAction::Approve,
			user_code: "ABCD-EFGH".into(),
			csrf: Some("csrf".into()),
			username: None,
			password: None,
		};

		let decision =
			validated_consent_decision(&form, Some(&session)).expect("decision validates");
		assert_eq!(decision.user_id, owned_user_id!("@alice:example.com"));
		assert_eq!(decision.user_code, "ABCD-EFGH");
		assert_eq!(decision.action, LinkAction::Approve);
	}

	#[tokio::test]
	async fn oauth_consent_invalid_user_code_is_error_page() {
		let response = user_code_error_response(StatusCode::NOT_FOUND);
		let (parts, body) = response.into_parts();
		assert_eq!(parts.status, StatusCode::NOT_FOUND);
		let body = to_bytes(body, usize::MAX)
			.await
			.expect("body reads");
		let body = std::str::from_utf8(&body).expect("body is utf8");
		assert!(body.contains("expired or invalid"));
	}

	#[test]
	fn oauth_consent_gate_off_error_is_unrecognized_404() {
		let error = consent_disabled_error();
		assert_eq!(error.status_code(), StatusCode::NOT_FOUND);
		assert_eq!(error.kind(), ruma::api::client::error::ErrorKind::Unrecognized,);
	}
}
