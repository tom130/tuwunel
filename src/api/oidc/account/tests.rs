use http::{StatusCode, header::CONTENT_SECURITY_POLICY};
use tuwunel_core::utils::html::TUWUNEL_CSP_VALUE;

use super::{
	ACCOUNT_CSS, ACCOUNT_HEAD, ACCOUNT_MANAGEMENT_ACTIONS_SUPPORTED,
	account_html_response_with_form_action, browser_error_page, normalize_account_action,
	redirect_origin,
};

#[test]
fn shared_assets_define_responsive_native_auth_theme() {
	assert!(ACCOUNT_HEAD.contains(r#"name="viewport""#));

	for rule in [
		"body.auth-page",
		".auth-card",
		".auth-form input[type=\"password\"]",
		".auth-form input:focus-visible",
		".auth-submit:focus-visible",
		"@media (max-width: 760px)",
		"@media (prefers-reduced-motion: reduce)",
	] {
		assert!(ACCOUNT_CSS.contains(rule), "missing auth theme rule: {rule}");
	}
}

#[test]
fn authorization_csp_only_widens_form_action() {
	let strict = account_html_response_with_form_action(StatusCode::OK, String::new(), None);
	let widened = account_html_response_with_form_action(
		StatusCode::OK,
		String::new(),
		Some("https://element.example"),
	);

	assert_eq!(strict.headers()[CONTENT_SECURITY_POLICY], TUWUNEL_CSP_VALUE);
	assert_eq!(
		widened.headers()[CONTENT_SECURITY_POLICY],
		"default-src 'none';script-src 'self';style-src 'self';frame-ancestors 'none';form-action 'self' https://element.example;base-uri 'none'"
	);
}

#[test]
fn browser_redirect_origin_is_http_or_https_only() {
	assert_eq!(
		redirect_origin("https://element.example:8448/callback?query=value").as_deref(),
		Some("https://element.example:8448")
	);
	assert_eq!(
		redirect_origin("http://element.example/callback").as_deref(),
		Some("http://element.example")
	);
	assert_eq!(redirect_origin("io.element.android:/callback"), None);
	assert_eq!(redirect_origin("javascript:alert(1)"), None);
	assert_eq!(redirect_origin("not a URI"), None);
}

#[test]
fn browser_error_page_escapes_message_and_uses_safe_fallback() {
	let html = browser_error_page("<script>alert(1)</script>", None);

	assert!(!html.contains("<script>"));
	assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
	assert!(html.contains(r#"href="/_tuwunel/oidc/account""#));
	assert!(!html.contains("errcode"));

	let html = browser_error_page("Try again", Some("https://element.example"));
	assert!(html.contains(r#"href="https://element.example""#));
}

#[test]
fn stable_names_map_to_aliases() {
	assert_eq!(normalize_account_action("org.matrix.devices_list"), "org.matrix.sessions_list");
	assert_eq!(normalize_account_action("org.matrix.device_view"), "org.matrix.session_view");
	assert_eq!(normalize_account_action("org.matrix.device_delete"), "org.matrix.session_end");
}

#[test]
fn aliases_and_others_pass_through() {
	for action in [
		"org.matrix.sessions_list",
		"org.matrix.session_view",
		"org.matrix.session_end",
		"org.matrix.profile",
		"org.matrix.account_deactivate",
		"org.matrix.cross_signing_reset",
	] {
		assert_eq!(normalize_account_action(action), action);
	}
}

#[test]
fn stable_actions_are_advertised() {
	for action in [
		"org.matrix.profile",
		"org.matrix.devices_list",
		"org.matrix.device_view",
		"org.matrix.device_delete",
		"org.matrix.account_deactivate",
		"org.matrix.cross_signing_reset",
	] {
		assert!(ACCOUNT_MANAGEMENT_ACTIONS_SUPPORTED.contains(&action));
	}
}
