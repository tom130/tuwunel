use axum::response::{Html, IntoResponse, Response};
use axum_extra::extract::cookie::{Cookie, CookieJar, SameSite};
use http::{
	HeaderValue, StatusCode,
	header::{CACHE_CONTROL, SET_COOKIE},
};
use tuwunel_core::Result;
use tuwunel_service::oauth_provider::consent::{BrowserSession, CreatedSession};
use url::Url;

const CONSENT_COOKIE: &str = "tuwunel_oauth_consent";
const CONSENT_COOKIE_PATH: &str = "/_tuwunel/oauth";
const MESSAGE_TEMPLATE: &str = include_str!("templates/consent_message.html");

pub(crate) async fn consent_session_from_cookie(
	services: &crate::State,
	jar: &CookieJar,
) -> Option<BrowserSession> {
	let session_id = jar.get(CONSENT_COOKIE)?.value();
	services
		.oauth_provider
		.consent
		.get(session_id)
		.await
		.ok()
}

pub(crate) fn append_consent_cookie(
	response: &mut Response,
	created: &CreatedSession,
	secure: bool,
) -> Result {
	let cookie = Cookie::build((CONSENT_COOKIE, created.session_id.clone()))
		.path(CONSENT_COOKIE_PATH)
		.http_only(true)
		.same_site(SameSite::Lax)
		.secure(secure)
		.build()
		.to_string();
	let cookie = HeaderValue::from_str(&cookie)?;
	response.headers_mut().append(SET_COOKIE, cookie);

	Ok(())
}

pub(crate) fn sso_provider_links(
	services: crate::State,
	base_url: &str,
	return_path: &str,
	return_params: &[(&str, &str)],
) -> Result<String> {
	let mut links = Vec::new();

	for provider in services.config.identity_provider.values() {
		let idp_id = provider.id();
		let name = provider
			.name
			.as_deref()
			.unwrap_or(provider.brand.as_str());
		let mut redirect_url = Url::parse(&format!("{base_url}{return_path}"))?;
		{
			let mut query = redirect_url.query_pairs_mut();
			for (name, value) in return_params {
				query.append_pair(name, value);
			}
		}

		let mut sso_url =
			Url::parse(&format!("{base_url}/_matrix/client/v3/login/sso/redirect/{idp_id}",))?;
		sso_url
			.query_pairs_mut()
			.append_pair("redirectUrl", redirect_url.as_str());

		links.push(format!(
			"<a class=\"sso\" href=\"{}\">Sign in with {}</a>",
			escape_html(sso_url.as_str()),
			escape_html(name),
		));
	}

	Ok(links.join("\n"))
}

pub(crate) fn message_response(status: StatusCode, title: &str, message: &str) -> Response {
	let body = MESSAGE_TEMPLATE
		.replace("{{TITLE}}", &escape_html(title))
		.replace("{{MESSAGE}}", &escape_html(message));

	html_response(status, body)
}

pub(crate) fn html_response(status: StatusCode, body: String) -> Response {
	let mut response = (status, Html(body)).into_response();
	response
		.headers_mut()
		.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
	response
}

pub(crate) fn public_base_url(services: crate::State) -> String {
	services
		.server
		.config
		.well_known
		.client
		.as_ref()
		.map(ToString::to_string)
		.unwrap_or_else(|| format!("https://{}", services.server.name))
		.trim_end_matches('/')
		.to_owned()
}

pub(crate) fn escape_html(value: &str) -> String {
	value
		.replace('&', "&amp;")
		.replace('<', "&lt;")
		.replace('>', "&gt;")
		.replace('"', "&quot;")
		.replace('\'', "&#39;")
}
